package manager

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/kris-nova/logger"

	api "github.com/weaveworks/eksctl/pkg/apis/eksctl.io/v1alpha4"
)

// Task is a common interface for the stack manager tasks
type Task interface {
	Do(chan error) error
	Describe() string
}

// TaskSet wraps a set of tasks
type TaskSet struct {
	tasks    []Task
	Parallel bool
	DryRun   bool
}

// Append new tasks to the set
func (t *TaskSet) Append(task ...Task) {
	t.tasks = append(t.tasks, task...)
}

// Len returns number of tasks in the set
func (t *TaskSet) Len() int {
	if t == nil {
		return 0
	}
	return len(t.tasks)
}

// Describe the set
func (t *TaskSet) Describe() string {
	descriptions := []string{}
	for _, task := range t.tasks {
		descriptions = append(descriptions, task.Describe())
	}
	mode := "sequential"
	if t.Parallel {
		mode = "parallel"
	}
	count := len(descriptions)
	var msg string
	switch count {
	case 0:
		msg = "no tasks to-do"
	case 1:
		msg = fmt.Sprintf("1 task to-do: { %s }", descriptions[0])
	default:
		msg = fmt.Sprintf("%d %s tasks to-do: { %s }", count, mode, strings.Join(descriptions, ", "))
	}
	if t.DryRun {
		return "(dry-run) " + msg
	}
	return msg
}

func (t *TaskSet) Do(errs chan error) error {
	if t.Len() == 0 || t.DryRun {
		logger.Debug("no actual tasks")
		close(errs)
		return nil
	}

	sendErr := func(err error) {
		errs <- err
	}

	if t.Parallel {
		go Run(sendErr, t.tasks...)
	} else {
		go func() {
			for _, task := range t.tasks {
				Run(sendErr, task)
			}
		}()
	}
	return nil
}

func (t *TaskSet) DoAll() []error {
	if t.Len() == 0 || t.DryRun {
		logger.Debug("no actual tasks")
		return nil
	}

	errs := []error{}
	appendErr := func(err error) {
		errs = append(errs, err)
	}

	if t.Parallel {
		Run(appendErr, t.tasks...)
	} else {
		for _, task := range t.tasks {
			Run(appendErr, task)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

type taskWithoutParams struct {
	info string
	call func(chan error) error
}

func (t *taskWithoutParams) Describe() string { return t.info }
func (t *taskWithoutParams) Do(errs chan error) error {
	return t.call(errs)
}

type taskWithNameParam struct {
	info string
	name string
	call func(chan error, string) error
}

func (t *taskWithNameParam) Describe() string { return t.info }
func (t *taskWithNameParam) Do(errs chan error) error {
	return t.call(errs, t.name)
}

type taskWithNodeGroupSpec struct {
	info      string
	nodeGroup *api.NodeGroup
	call      func(chan error, *api.NodeGroup) error
}

func (t *taskWithNodeGroupSpec) Describe() string { return t.info }
func (t *taskWithNodeGroupSpec) Do(errs chan error) error {
	return t.call(errs, t.nodeGroup)
}

type taskWithStackSpec struct {
	info  string
	stack *Stack
	call  func(*Stack, chan error) error
}

func (t *taskWithStackSpec) Describe() string { return t.info }
func (t *taskWithStackSpec) Do(errs chan error) error {
	return t.call(t.stack, errs)
}

type asyncTaskWithStackSpec struct {
	info  string
	stack *Stack
	call  func(*Stack) (*Stack, error)
}

func (t *asyncTaskWithStackSpec) Describe() string { return t.info + " [async]" }
func (t *asyncTaskWithStackSpec) Do(errs chan error) error {
	_, err := t.call(t.stack)
	close(errs)
	return err
}

// Run a series of tasks in parallel and wait for each task them to complete;
// passError should take any errors and do what it needs to in
// a given context, e.g. during serial CLI-driven execution one
// can keep errors in a slice, while in a daemon channel maybe
// more suitable
func Run(passError func(error), tasks ...Task) {
	wg := &sync.WaitGroup{}
	wg.Add(len(tasks))
	for t := range tasks {
		go func(t int) {
			defer wg.Done()
			logger.Debug("task %d started - %s", t, tasks[t].Describe())
			errs := make(chan error)
			if err := tasks[t].Do(errs); err != nil {
				passError(err)
				return
			}
			if err := <-errs; err != nil {
				passError(err)
				return
			}
			logger.Debug("task %d returned without errors", t)
		}(t)
	}
	logger.Debug("waiting for %d tasks to complete", len(tasks))
	wg.Wait()
}

// CreateTasksForClusterWithNodeGroups defines all tasks required to create a cluster along
// with some nodegroups; see CreateAllNodeGroups for how onlyNodeGroupSubset works
func (c *StackCollection) CreateTasksForClusterWithNodeGroups(onlyNodeGroupSubset sets.String) *TaskSet {
	tasks := &TaskSet{Parallel: false}

	tasks.Append(
		&taskWithoutParams{
			call: c.createClusterTask,
		},
		c.CreateTasksForNodeGroups(onlyNodeGroupSubset),
	)

	return tasks
}

// CreateTasksForNodeGroups defines task required to create all of the nodegroups if
// onlySubset can be nil, otherwise just the nodegroups that are in onlySubset will be created
func (c *StackCollection) CreateTasksForNodeGroups(onlySubset sets.String) *TaskSet {
	tasks := &TaskSet{Parallel: true}

	for i := range c.spec.NodeGroups {
		ng := c.spec.NodeGroups[i]
		if onlySubset != nil && !onlySubset.Has(ng.Name) {
			continue
		}
		tasks.Append(&taskWithNodeGroupSpec{
			info:      fmt.Sprintf("create nodegroup %q", ng.Name),
			nodeGroup: ng,
			call:      c.createNodeGroupTask,
		})
	}

	return tasks
}

// DeleteTasksForClusterWithNodeGroups defines tasks required to delete all the nodegroup stacks and the cluster
func (c *StackCollection) DeleteTasksForClusterWithNodeGroups(onlySubset sets.String, wait bool, cleanup func(chan error, string) error) (*TaskSet, error) {
	tasks := &TaskSet{Parallel: false}

	nodeGroupTasks, err := c.DeleteTasksForNodeGroups(onlySubset, true, cleanup)
	if err != nil {
		return nil, err
	}
	tasks.Append(nodeGroupTasks)

	clusterStack, err := c.DescribeClusterStack()
	if err != nil {
		return nil, err
	}

	info := fmt.Sprintf("delete control plane %q", c.spec.Metadata.Name)
	if wait {
		tasks.Append(&taskWithStackSpec{
			info:  info,
			stack: clusterStack,
			call:  c.WaitDeleteStackBySpec,
		})
	} else {
		tasks.Append(&asyncTaskWithStackSpec{
			info:  info,
			stack: clusterStack,
			call:  c.DeleteStackBySpec,
		})
	}

	return tasks, nil
}

// DeleteTasksForNodeGroups defines tasks required to delete all the nodegroup stacks
func (c *StackCollection) DeleteTasksForNodeGroups(onlySubset sets.String, wait bool, cleanup func(chan error, string) error) (*TaskSet, error) {
	nodeGroupStacks, err := c.DescribeNodeGroupStacks()
	if err != nil {
		return nil, err
	}

	tasks := &TaskSet{Parallel: true}

	for _, s := range nodeGroupStacks {
		name := getNodeGroupName(s)
		if onlySubset != nil && !onlySubset.Has(name) {
			continue
		}
		if *s.StackStatus == cloudformation.StackStatusDeleteFailed && cleanup != nil {
			tasks.Append(&taskWithNameParam{
				info: fmt.Sprintf("cleanup for nodegroup %q", name),
				call: cleanup,
			})
		}
		if wait {
			tasks.Append(&taskWithStackSpec{
				info:  fmt.Sprintf("delete nodegroup %q", name),
				stack: s,
				call:  c.WaitDeleteStackBySpec,
			})
		} else {
			tasks.Append(&asyncTaskWithStackSpec{
				info:  fmt.Sprintf("delete nodegroup %q", name),
				stack: s,
				call:  c.DeleteStackBySpec,
			})
		}
	}

	return tasks, nil
}
