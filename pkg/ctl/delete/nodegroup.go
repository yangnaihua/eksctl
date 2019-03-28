package delete

import (
	"fmt"
	"os"

	"github.com/kris-nova/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	api "github.com/weaveworks/eksctl/pkg/apis/eksctl.io/v1alpha4"
	"github.com/weaveworks/eksctl/pkg/authconfigmap"
	"github.com/weaveworks/eksctl/pkg/ctl/cmdutils"
	"github.com/weaveworks/eksctl/pkg/drain"
	"github.com/weaveworks/eksctl/pkg/eks"
)

var (
	updateAuthConfigMap  bool
	deleteNodeGroupDrain bool

	nodeGroupOnlyFilters        []string
	deleteOnlyMissingNodeGroups bool
)

func deleteNodeGroupCmd(g *cmdutils.Grouping) *cobra.Command {
	p := &api.ProviderConfig{}
	cfg := api.NewClusterConfig()
	ng := cfg.NewNodeGroup()

	cmd := &cobra.Command{
		Use:     "nodegroup",
		Short:   "Delete a nodegroup",
		Aliases: []string{"ng"},
		Run: func(cmd *cobra.Command, args []string) {
			if err := doDeleteNodeGroup(p, cfg, ng, cmdutils.GetNameArg(args), cmd); err != nil {
				logger.Critical("%s\n", err.Error())
				os.Exit(1)
			}
		},
	}

	group := g.New(cmd)

	group.InFlagSet("General", func(fs *pflag.FlagSet) {
		fs.StringVar(&cfg.Metadata.Name, "cluster", "", "EKS cluster name")
		cmdutils.AddRegionFlag(fs, p)
		fs.StringVarP(&ng.Name, "name", "n", "", "Name of the nodegroup to delete")
		cmdutils.AddConfigFileFlag(&clusterConfigFile, fs)
		cmdutils.AddNodeGroupsOnlyFlag(&nodeGroupOnlyFilters, fs)
		fs.BoolVar(&deleteOnlyMissingNodeGroups, "only-missing", false, "Only delete nodegroups that are missing from the given config file")
		cmdutils.AddUpdateAuthConfigMap(&updateAuthConfigMap, fs, "Remove nodegroup IAM role from aws-auth configmap")
		fs.BoolVar(&deleteNodeGroupDrain, "drain", true, "Drain and cordon all nodes in the nodegroup before deletion")
		cmdutils.AddWaitFlag(&wait, fs, "deletion of all resources")
	})

	cmdutils.AddCommonFlagsForAWS(group, p, true)

	group.AddTo(cmd)

	return cmd
}

func doDeleteNodeGroup(p *api.ProviderConfig, cfg *api.ClusterConfig, ng *api.NodeGroup, nameArg string, cmd *cobra.Command) error {
	ngFilter := cmdutils.NewNodeGroupFilter()

	if deleteOnlyMissingNodeGroups {
		ngFilter.SkipAll = true
	}

	if err := cmdutils.NewDeleteNodeGroupLoader(p, cfg, ng, clusterConfigFile, nameArg, cmd, ngFilter, nodeGroupOnlyFilters).Load(); err != nil {
		return err
	}

	ctl := eks.New(p, cfg)

	if err := ctl.CheckAuth(); err != nil {
		return err
	}

	if err := ctl.GetCredentials(cfg); err != nil {
		return errors.Wrapf(err, "getting credentials for cluster %q", cfg.Metadata.Name)
	}

	clientSet, err := ctl.NewStdClientSet(cfg)
	if err != nil {
		return err
	}

	if updateAuthConfigMap {
		// remove node group from config map
		if err := authconfigmap.RemoveNodeGroup(clientSet, ng); err != nil {
			logger.Warning(err.Error())
		}
	}

	if deleteNodeGroupDrain {
		if err := drain.NodeGroup(clientSet, ng, ctl.Provider.WaitTimeout(), false); err != nil {
			return err
		}
	}

	stackManager := ctl.NewStackManager(cfg)

	_ = ngFilter.CheckEachNodeGroup(cfg.NodeGroups, func(_ int, ng *api.NodeGroup) error {
		if ng.IAM.InstanceRoleARN == "" {
			if err := ctl.GetNodeGroupIAM(cfg, ng); err != nil {
				logger.Warning("%s getting instance role ARN for node group %q", err.Error(), ng.Name)
			}
		}
		return nil
	})

	ngSubset := ngFilter.MatchAll(cfg)

	if ngSubset.Len() == 0 && !deleteOnlyMissingNodeGroups {
		ngFilter.LogInfo(cfg)
		return nil
	}

	if !wait {
		err := ngFilter.CheckEachNodeGroup(cfg.NodeGroups, func(_ int, ng *api.NodeGroup) error {
			logger.Info("will delete nodegroup %q in cluster %q", ng.Name, cfg.Metadata.Name)
			if err := stackManager.DeleteNodeGroup(ng.Name); err != nil {
				return errors.Wrapf(err, "failed to delete nodegroup %q", ng.Name)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	errs := stackManager.WaitDeleteAllNodeGroups(false, ngSubset)
	if len(errs) > 0 {
		logger.Info("%d error(s) occurred while deleting nodegroup(s)", len(errs))
		for _, err := range errs {
			logger.Critical("%s\n", err.Error())
		}
		return fmt.Errorf("failed to delete nodegroup(s)")
	}

	if deleteOnlyMissingNodeGroups {
		// 1. get the list
		// 2. get IAM ARNs and update auth configmap
		// 3. delete (blocking and non-blocking)
		// TODO: rename CheckEachNodeGroup to ForEachNodeGroup
	}

	return nil
}
