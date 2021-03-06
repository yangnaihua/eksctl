package cloudformation

import (
	"encoding/json"
)

// AWSEMRCluster_ScalingConstraints AWS CloudFormation Resource (AWS::EMR::Cluster.ScalingConstraints)
// See: http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-elasticmapreduce-cluster-scalingconstraints.html
type AWSEMRCluster_ScalingConstraints struct {

	// MaxCapacity AWS CloudFormation Property
	// Required: true
	// See: http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-elasticmapreduce-cluster-scalingconstraints.html#cfn-elasticmapreduce-cluster-scalingconstraints-maxcapacity
	MaxCapacity *Value `json:"MaxCapacity,omitempty"`

	// MinCapacity AWS CloudFormation Property
	// Required: true
	// See: http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-elasticmapreduce-cluster-scalingconstraints.html#cfn-elasticmapreduce-cluster-scalingconstraints-mincapacity
	MinCapacity *Value `json:"MinCapacity,omitempty"`
}

// AWSCloudFormationType returns the AWS CloudFormation resource type
func (r *AWSEMRCluster_ScalingConstraints) AWSCloudFormationType() string {
	return "AWS::EMR::Cluster.ScalingConstraints"
}

func (r *AWSEMRCluster_ScalingConstraints) MarshalJSON() ([]byte, error) {
	return json.Marshal(*r)
}
