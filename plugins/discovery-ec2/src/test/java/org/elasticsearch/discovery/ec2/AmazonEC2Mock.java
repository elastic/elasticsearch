/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AcceptVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.AcceptVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.AllocateAddressRequest;
import com.amazonaws.services.ec2.model.AllocateAddressResult;
import com.amazonaws.services.ec2.model.AllocateHostsRequest;
import com.amazonaws.services.ec2.model.AllocateHostsResult;
import com.amazonaws.services.ec2.model.AssignPrivateIpAddressesRequest;
import com.amazonaws.services.ec2.model.AssignPrivateIpAddressesResult;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.AssociateAddressResult;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsResult;
import com.amazonaws.services.ec2.model.AssociateRouteTableRequest;
import com.amazonaws.services.ec2.model.AssociateRouteTableResult;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcRequest;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcResult;
import com.amazonaws.services.ec2.model.AttachInternetGatewayRequest;
import com.amazonaws.services.ec2.model.AttachInternetGatewayResult;
import com.amazonaws.services.ec2.model.AttachNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.AttachNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.AttachVolumeResult;
import com.amazonaws.services.ec2.model.AttachVpnGatewayRequest;
import com.amazonaws.services.ec2.model.AttachVpnGatewayResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressResult;
import com.amazonaws.services.ec2.model.BundleInstanceRequest;
import com.amazonaws.services.ec2.model.BundleInstanceResult;
import com.amazonaws.services.ec2.model.CancelBundleTaskRequest;
import com.amazonaws.services.ec2.model.CancelBundleTaskResult;
import com.amazonaws.services.ec2.model.CancelConversionTaskRequest;
import com.amazonaws.services.ec2.model.CancelConversionTaskResult;
import com.amazonaws.services.ec2.model.CancelExportTaskRequest;
import com.amazonaws.services.ec2.model.CancelExportTaskResult;
import com.amazonaws.services.ec2.model.CancelImportTaskRequest;
import com.amazonaws.services.ec2.model.CancelImportTaskResult;
import com.amazonaws.services.ec2.model.CancelReservedInstancesListingRequest;
import com.amazonaws.services.ec2.model.CancelReservedInstancesListingResult;
import com.amazonaws.services.ec2.model.CancelSpotFleetRequestsRequest;
import com.amazonaws.services.ec2.model.CancelSpotFleetRequestsResult;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.ConfirmProductInstanceRequest;
import com.amazonaws.services.ec2.model.ConfirmProductInstanceResult;
import com.amazonaws.services.ec2.model.CopyImageRequest;
import com.amazonaws.services.ec2.model.CopyImageResult;
import com.amazonaws.services.ec2.model.CopySnapshotRequest;
import com.amazonaws.services.ec2.model.CopySnapshotResult;
import com.amazonaws.services.ec2.model.CreateCustomerGatewayRequest;
import com.amazonaws.services.ec2.model.CreateCustomerGatewayResult;
import com.amazonaws.services.ec2.model.CreateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.CreateDhcpOptionsResult;
import com.amazonaws.services.ec2.model.CreateFlowLogsRequest;
import com.amazonaws.services.ec2.model.CreateFlowLogsResult;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.CreateInstanceExportTaskRequest;
import com.amazonaws.services.ec2.model.CreateInstanceExportTaskResult;
import com.amazonaws.services.ec2.model.CreateInternetGatewayRequest;
import com.amazonaws.services.ec2.model.CreateInternetGatewayResult;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateNatGatewayRequest;
import com.amazonaws.services.ec2.model.CreateNatGatewayResult;
import com.amazonaws.services.ec2.model.CreateNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.CreateNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.CreateNetworkAclRequest;
import com.amazonaws.services.ec2.model.CreateNetworkAclResult;
import com.amazonaws.services.ec2.model.CreateNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.CreateNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.CreatePlacementGroupRequest;
import com.amazonaws.services.ec2.model.CreatePlacementGroupResult;
import com.amazonaws.services.ec2.model.CreateReservedInstancesListingRequest;
import com.amazonaws.services.ec2.model.CreateReservedInstancesListingResult;
import com.amazonaws.services.ec2.model.CreateRouteRequest;
import com.amazonaws.services.ec2.model.CreateRouteResult;
import com.amazonaws.services.ec2.model.CreateRouteTableRequest;
import com.amazonaws.services.ec2.model.CreateRouteTableResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateSnapshotRequest;
import com.amazonaws.services.ec2.model.CreateSnapshotResult;
import com.amazonaws.services.ec2.model.CreateSpotDatafeedSubscriptionRequest;
import com.amazonaws.services.ec2.model.CreateSpotDatafeedSubscriptionResult;
import com.amazonaws.services.ec2.model.CreateSubnetRequest;
import com.amazonaws.services.ec2.model.CreateSubnetResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateTagsResult;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.CreateVpcEndpointRequest;
import com.amazonaws.services.ec2.model.CreateVpcEndpointResult;
import com.amazonaws.services.ec2.model.CreateVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.CreateVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.CreateVpcRequest;
import com.amazonaws.services.ec2.model.CreateVpcResult;
import com.amazonaws.services.ec2.model.CreateVpnConnectionRequest;
import com.amazonaws.services.ec2.model.CreateVpnConnectionResult;
import com.amazonaws.services.ec2.model.CreateVpnConnectionRouteRequest;
import com.amazonaws.services.ec2.model.CreateVpnConnectionRouteResult;
import com.amazonaws.services.ec2.model.CreateVpnGatewayRequest;
import com.amazonaws.services.ec2.model.CreateVpnGatewayResult;
import com.amazonaws.services.ec2.model.DeleteCustomerGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteCustomerGatewayResult;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsResult;
import com.amazonaws.services.ec2.model.DeleteFlowLogsRequest;
import com.amazonaws.services.ec2.model.DeleteFlowLogsResult;
import com.amazonaws.services.ec2.model.DeleteInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteInternetGatewayResult;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairResult;
import com.amazonaws.services.ec2.model.DeleteNatGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteNatGatewayResult;
import com.amazonaws.services.ec2.model.DeleteNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.DeleteNetworkAclRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclResult;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.DeletePlacementGroupRequest;
import com.amazonaws.services.ec2.model.DeletePlacementGroupResult;
import com.amazonaws.services.ec2.model.DeleteRouteRequest;
import com.amazonaws.services.ec2.model.DeleteRouteResult;
import com.amazonaws.services.ec2.model.DeleteRouteTableRequest;
import com.amazonaws.services.ec2.model.DeleteRouteTableResult;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupResult;
import com.amazonaws.services.ec2.model.DeleteSnapshotRequest;
import com.amazonaws.services.ec2.model.DeleteSnapshotResult;
import com.amazonaws.services.ec2.model.DeleteSpotDatafeedSubscriptionRequest;
import com.amazonaws.services.ec2.model.DeleteSpotDatafeedSubscriptionResult;
import com.amazonaws.services.ec2.model.DeleteSubnetRequest;
import com.amazonaws.services.ec2.model.DeleteSubnetResult;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsResult;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DeleteVolumeResult;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsResult;
import com.amazonaws.services.ec2.model.DeleteVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.DeleteVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.DeleteVpcRequest;
import com.amazonaws.services.ec2.model.DeleteVpcResult;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionRequest;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionResult;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionRouteRequest;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionRouteResult;
import com.amazonaws.services.ec2.model.DeleteVpnGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteVpnGatewayResult;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DeregisterImageResult;
import com.amazonaws.services.ec2.model.DescribeAccountAttributesRequest;
import com.amazonaws.services.ec2.model.DescribeAccountAttributesResult;
import com.amazonaws.services.ec2.model.DescribeAddressesRequest;
import com.amazonaws.services.ec2.model.DescribeAddressesResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeBundleTasksRequest;
import com.amazonaws.services.ec2.model.DescribeBundleTasksResult;
import com.amazonaws.services.ec2.model.DescribeClassicLinkInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeClassicLinkInstancesResult;
import com.amazonaws.services.ec2.model.DescribeConversionTasksRequest;
import com.amazonaws.services.ec2.model.DescribeConversionTasksResult;
import com.amazonaws.services.ec2.model.DescribeCustomerGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeCustomerGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DescribeDhcpOptionsResult;
import com.amazonaws.services.ec2.model.DescribeExportTasksRequest;
import com.amazonaws.services.ec2.model.DescribeExportTasksResult;
import com.amazonaws.services.ec2.model.DescribeFlowLogsRequest;
import com.amazonaws.services.ec2.model.DescribeFlowLogsResult;
import com.amazonaws.services.ec2.model.DescribeHostsRequest;
import com.amazonaws.services.ec2.model.DescribeHostsResult;
import com.amazonaws.services.ec2.model.DescribeIdentityIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribeIdentityIdFormatResult;
import com.amazonaws.services.ec2.model.DescribeIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribeIdFormatResult;
import com.amazonaws.services.ec2.model.DescribeImageAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeImageAttributeResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeImportImageTasksRequest;
import com.amazonaws.services.ec2.model.DescribeImportImageTasksResult;
import com.amazonaws.services.ec2.model.DescribeImportSnapshotTasksRequest;
import com.amazonaws.services.ec2.model.DescribeImportSnapshotTasksResult;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeResult;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeInternetGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeInternetGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeMovingAddressesRequest;
import com.amazonaws.services.ec2.model.DescribeMovingAddressesResult;
import com.amazonaws.services.ec2.model.DescribeNatGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeNatGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsResult;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfaceAttributeResult;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacesRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacesResult;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsRequest;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsResult;
import com.amazonaws.services.ec2.model.DescribePrefixListsRequest;
import com.amazonaws.services.ec2.model.DescribePrefixListsResult;
import com.amazonaws.services.ec2.model.DescribeRegionsRequest;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesListingsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesListingsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesOfferingsRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesOfferingsResult;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeReservedInstancesResult;
import com.amazonaws.services.ec2.model.DescribeRouteTablesRequest;
import com.amazonaws.services.ec2.model.DescribeRouteTablesResult;
import com.amazonaws.services.ec2.model.DescribeScheduledInstanceAvailabilityRequest;
import com.amazonaws.services.ec2.model.DescribeScheduledInstanceAvailabilityResult;
import com.amazonaws.services.ec2.model.DescribeScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeScheduledInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupReferencesRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupReferencesResult;
import com.amazonaws.services.ec2.model.DescribeSnapshotAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotAttributeResult;
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsResult;
import com.amazonaws.services.ec2.model.DescribeSpotDatafeedSubscriptionRequest;
import com.amazonaws.services.ec2.model.DescribeSpotDatafeedSubscriptionResult;
import com.amazonaws.services.ec2.model.DescribeSpotFleetInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeSpotFleetInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSpotFleetRequestHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotFleetRequestHistoryResult;
import com.amazonaws.services.ec2.model.DescribeSpotFleetRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotFleetRequestsResult;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.DescribeStaleSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeStaleSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.DescribeTagsRequest;
import com.amazonaws.services.ec2.model.DescribeTagsResult;
import com.amazonaws.services.ec2.model.DescribeVolumeAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeVolumeAttributeResult;
import com.amazonaws.services.ec2.model.DescribeVolumeStatusRequest;
import com.amazonaws.services.ec2.model.DescribeVolumeStatusResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.DescribeVpcAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeVpcAttributeResult;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServicesRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServicesResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsResult;
import com.amazonaws.services.ec2.model.DescribeVpcPeeringConnectionsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcPeeringConnectionsResult;
import com.amazonaws.services.ec2.model.DescribeVpcsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.amazonaws.services.ec2.model.DescribeVpnConnectionsRequest;
import com.amazonaws.services.ec2.model.DescribeVpnConnectionsResult;
import com.amazonaws.services.ec2.model.DescribeVpnGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeVpnGatewaysResult;
import com.amazonaws.services.ec2.model.DetachClassicLinkVpcRequest;
import com.amazonaws.services.ec2.model.DetachClassicLinkVpcResult;
import com.amazonaws.services.ec2.model.DetachInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DetachInternetGatewayResult;
import com.amazonaws.services.ec2.model.DetachNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DetachNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
import com.amazonaws.services.ec2.model.DetachVolumeResult;
import com.amazonaws.services.ec2.model.DetachVpnGatewayRequest;
import com.amazonaws.services.ec2.model.DetachVpnGatewayResult;
import com.amazonaws.services.ec2.model.DisableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.DisableVgwRoutePropagationResult;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.DisassociateAddressResult;
import com.amazonaws.services.ec2.model.DisassociateRouteTableRequest;
import com.amazonaws.services.ec2.model.DisassociateRouteTableResult;
import com.amazonaws.services.ec2.model.DryRunResult;
import com.amazonaws.services.ec2.model.DryRunSupportedRequest;
import com.amazonaws.services.ec2.model.EnableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.EnableVgwRoutePropagationResult;
import com.amazonaws.services.ec2.model.EnableVolumeIORequest;
import com.amazonaws.services.ec2.model.EnableVolumeIOResult;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GetConsoleOutputRequest;
import com.amazonaws.services.ec2.model.GetConsoleOutputResult;
import com.amazonaws.services.ec2.model.GetConsoleScreenshotRequest;
import com.amazonaws.services.ec2.model.GetConsoleScreenshotResult;
import com.amazonaws.services.ec2.model.GetPasswordDataRequest;
import com.amazonaws.services.ec2.model.GetPasswordDataResult;
import com.amazonaws.services.ec2.model.ImportImageRequest;
import com.amazonaws.services.ec2.model.ImportImageResult;
import com.amazonaws.services.ec2.model.ImportInstanceRequest;
import com.amazonaws.services.ec2.model.ImportInstanceResult;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.ec2.model.ImportKeyPairResult;
import com.amazonaws.services.ec2.model.ImportSnapshotRequest;
import com.amazonaws.services.ec2.model.ImportSnapshotResult;
import com.amazonaws.services.ec2.model.ImportVolumeRequest;
import com.amazonaws.services.ec2.model.ImportVolumeResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.ModifyHostsRequest;
import com.amazonaws.services.ec2.model.ModifyHostsResult;
import com.amazonaws.services.ec2.model.ModifyIdFormatRequest;
import com.amazonaws.services.ec2.model.ModifyIdFormatResult;
import com.amazonaws.services.ec2.model.ModifyIdentityIdFormatRequest;
import com.amazonaws.services.ec2.model.ModifyIdentityIdFormatResult;
import com.amazonaws.services.ec2.model.ModifyImageAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyImageAttributeResult;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeResult;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementRequest;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementResult;
import com.amazonaws.services.ec2.model.ModifyNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyNetworkInterfaceAttributeResult;
import com.amazonaws.services.ec2.model.ModifyReservedInstancesRequest;
import com.amazonaws.services.ec2.model.ModifyReservedInstancesResult;
import com.amazonaws.services.ec2.model.ModifySnapshotAttributeRequest;
import com.amazonaws.services.ec2.model.ModifySnapshotAttributeResult;
import com.amazonaws.services.ec2.model.ModifySpotFleetRequestRequest;
import com.amazonaws.services.ec2.model.ModifySpotFleetRequestResult;
import com.amazonaws.services.ec2.model.ModifySubnetAttributeRequest;
import com.amazonaws.services.ec2.model.ModifySubnetAttributeResult;
import com.amazonaws.services.ec2.model.ModifyVolumeAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVolumeAttributeResult;
import com.amazonaws.services.ec2.model.ModifyVpcAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVpcAttributeResult;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointResult;
import com.amazonaws.services.ec2.model.ModifyVpcPeeringConnectionOptionsRequest;
import com.amazonaws.services.ec2.model.ModifyVpcPeeringConnectionOptionsResult;
import com.amazonaws.services.ec2.model.MonitorInstancesRequest;
import com.amazonaws.services.ec2.model.MonitorInstancesResult;
import com.amazonaws.services.ec2.model.MoveAddressToVpcRequest;
import com.amazonaws.services.ec2.model.MoveAddressToVpcResult;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingRequest;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingResult;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesResult;
import com.amazonaws.services.ec2.model.RebootInstancesRequest;
import com.amazonaws.services.ec2.model.RebootInstancesResult;
import com.amazonaws.services.ec2.model.RegisterImageRequest;
import com.amazonaws.services.ec2.model.RegisterImageResult;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.ReleaseAddressRequest;
import com.amazonaws.services.ec2.model.ReleaseAddressResult;
import com.amazonaws.services.ec2.model.ReleaseHostsRequest;
import com.amazonaws.services.ec2.model.ReleaseHostsResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.ReplaceRouteRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteResult;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationResult;
import com.amazonaws.services.ec2.model.ReportInstanceStatusRequest;
import com.amazonaws.services.ec2.model.ReportInstanceStatusResult;
import com.amazonaws.services.ec2.model.RequestSpotFleetRequest;
import com.amazonaws.services.ec2.model.RequestSpotFleetResult;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResetImageAttributeRequest;
import com.amazonaws.services.ec2.model.ResetImageAttributeResult;
import com.amazonaws.services.ec2.model.ResetInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.ResetInstanceAttributeResult;
import com.amazonaws.services.ec2.model.ResetNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.ResetNetworkInterfaceAttributeResult;
import com.amazonaws.services.ec2.model.ResetSnapshotAttributeRequest;
import com.amazonaws.services.ec2.model.ResetSnapshotAttributeResult;
import com.amazonaws.services.ec2.model.RestoreAddressToClassicRequest;
import com.amazonaws.services.ec2.model.RestoreAddressToClassicResult;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressResult;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.RunScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.RunScheduledInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.model.UnassignPrivateIpAddressesRequest;
import com.amazonaws.services.ec2.model.UnassignPrivateIpAddressesResult;
import com.amazonaws.services.ec2.model.UnmonitorInstancesRequest;
import com.amazonaws.services.ec2.model.UnmonitorInstancesResult;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonEC2Mock implements AmazonEC2 {

    private static final Logger logger = ESLoggerFactory.getLogger(AmazonEC2Mock.class.getName());

    public static final String PREFIX_PRIVATE_IP = "10.0.0.";
    public static final String PREFIX_PUBLIC_IP = "8.8.8.";
    public static final String PREFIX_PUBLIC_DNS = "mock-ec2-";
    public static final String SUFFIX_PUBLIC_DNS = ".amazon.com";
    public static final String PREFIX_PRIVATE_DNS = "mock-ip-";
    public static final String SUFFIX_PRIVATE_DNS = ".ec2.internal";

    List<Instance> instances = new ArrayList<>();

    public AmazonEC2Mock(int nodes, List<List<Tag>> tagsList) {
        if (tagsList != null) {
            assert tagsList.size() == nodes;
        }

        for (int node = 1; node < nodes + 1; node++) {
            String instanceId = "node" + node;

            Instance instance = new Instance()
                    .withInstanceId(instanceId)
                    .withState(new InstanceState().withName(InstanceStateName.Running))
                    .withPrivateDnsName(PREFIX_PRIVATE_DNS + instanceId + SUFFIX_PRIVATE_DNS)
                    .withPublicDnsName(PREFIX_PUBLIC_DNS + instanceId + SUFFIX_PUBLIC_DNS)
                    .withPrivateIpAddress(PREFIX_PRIVATE_IP + node)
                    .withPublicIpAddress(PREFIX_PUBLIC_IP + node);

            if (tagsList != null) {
                instance.setTags(tagsList.get(node-1));
            }

            instances.add(instance);
        }

    }

    @Override
    public DescribeInstancesResult describeInstances(DescribeInstancesRequest describeInstancesRequest) throws AmazonServiceException, AmazonClientException {
        Collection<Instance> filteredInstances = new ArrayList<>();

        logger.debug("--> mocking describeInstances");

        for (Instance instance : instances) {
            boolean tagFiltered = false;
            boolean instanceFound = false;

            Map<String, List<String>> expectedTags = new HashMap<>();
            Map<String, List<String>> instanceTags = new HashMap<>();

            for (Tag tag : instance.getTags()) {
                List<String> tags = instanceTags.get(tag.getKey());
                if (tags == null) {
                    tags = new ArrayList<>();
                    instanceTags.put(tag.getKey(), tags);
                }
                tags.add(tag.getValue());
            }

            for (Filter filter : describeInstancesRequest.getFilters()) {
                // If we have the same tag name and one of the values, we add the instance
                if (filter.getName().startsWith("tag:")) {
                    tagFiltered = true;
                    String tagName = filter.getName().substring(4);
                    // if we have more than one value for the same key, then the key is appended with .x
                    Pattern p = Pattern.compile("\\.\\d+", Pattern.DOTALL);
                    Matcher m = p.matcher(tagName);
                    if (m.find()) {
                        int i = tagName.lastIndexOf(".");
                        tagName = tagName.substring(0, i);
                    }

                    List<String> tags = expectedTags.get(tagName);
                    if (tags == null) {
                        tags = new ArrayList<>();
                        expectedTags.put(tagName, tags);
                    }
                    tags.addAll(filter.getValues());
                }
            }

            if (tagFiltered) {
                logger.debug("--> expected tags: [{}]", expectedTags);
                logger.debug("--> instance tags: [{}]", instanceTags);

                instanceFound = true;
                for (Map.Entry<String, List<String>> expectedTagsEntry : expectedTags.entrySet()) {
                    List<String> instanceTagValues = instanceTags.get(expectedTagsEntry.getKey());
                    if (instanceTagValues == null) {
                        instanceFound = false;
                        break;
                    }

                    for (String expectedValue : expectedTagsEntry.getValue()) {
                        boolean valueFound = false;
                        for (String instanceTagValue : instanceTagValues) {
                            if (instanceTagValue.equals(expectedValue)) {
                                valueFound = true;
                            }
                        }
                        if (valueFound == false) {
                            instanceFound = false;
                        }
                    }
                }
            }

            if (tagFiltered == false || instanceFound) {
                logger.debug("--> instance added");
                filteredInstances.add(instance);
            } else {
                logger.debug("--> instance filtered");
            }
        }

        return new DescribeInstancesResult().withReservations(
                new Reservation().withInstances(filteredInstances)
        );
    }

    // Not implemented methods in Mock
    @Override
    public void setEndpoint(String endpoint) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void setRegion(com.amazonaws.regions.Region region) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection(
            AcceptVpcPeeringConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection() {
        return acceptVpcPeeringConnection(new AcceptVpcPeeringConnectionRequest());
    }

    @Override
    public AllocateAddressResult allocateAddress(AllocateAddressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateAddressResult allocateAddress() {
        return allocateAddress(new AllocateAddressRequest());
    }

    @Override
    public AllocateHostsResult allocateHosts(AllocateHostsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssignPrivateIpAddressesResult assignPrivateIpAddresses(
            AssignPrivateIpAddressesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateAddressResult associateAddress(
            AssociateAddressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateDhcpOptionsResult associateDhcpOptions(
            AssociateDhcpOptionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateRouteTableResult associateRouteTable(
            AssociateRouteTableRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachClassicLinkVpcResult attachClassicLinkVpc(
            AttachClassicLinkVpcRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachInternetGatewayResult attachInternetGateway(
            AttachInternetGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachNetworkInterfaceResult attachNetworkInterface(
            AttachNetworkInterfaceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVolumeResult attachVolume(AttachVolumeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVpnGatewayResult attachVpnGateway(
            AttachVpnGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AuthorizeSecurityGroupEgressResult authorizeSecurityGroupEgress(
            AuthorizeSecurityGroupEgressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AuthorizeSecurityGroupIngressResult authorizeSecurityGroupIngress(
            AuthorizeSecurityGroupIngressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public BundleInstanceResult bundleInstance(BundleInstanceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelBundleTaskResult cancelBundleTask(
            CancelBundleTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelConversionTaskResult cancelConversionTask(
            CancelConversionTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelExportTaskResult cancelExportTask(
            CancelExportTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask(
            CancelImportTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask() {
        return cancelImportTask(new CancelImportTaskRequest());
    }

    @Override
    public CancelReservedInstancesListingResult cancelReservedInstancesListing(
            CancelReservedInstancesListingRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotFleetRequestsResult cancelSpotFleetRequests(
            CancelSpotFleetRequestsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotInstanceRequestsResult cancelSpotInstanceRequests(
            CancelSpotInstanceRequestsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ConfirmProductInstanceResult confirmProductInstance(
            ConfirmProductInstanceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopyImageResult copyImage(CopyImageRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopySnapshotResult copySnapshot(CopySnapshotRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateCustomerGatewayResult createCustomerGateway(
            CreateCustomerGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateDhcpOptionsResult createDhcpOptions(
            CreateDhcpOptionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateFlowLogsResult createFlowLogs(CreateFlowLogsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateImageResult createImage(CreateImageRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInstanceExportTaskResult createInstanceExportTask(
            CreateInstanceExportTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway(
            CreateInternetGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway() {
        return createInternetGateway(new CreateInternetGatewayRequest());
    }

    @Override
    public CreateKeyPairResult createKeyPair(CreateKeyPairRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNatGatewayResult createNatGateway(
            CreateNatGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkAclResult createNetworkAcl(
            CreateNetworkAclRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkAclEntryResult createNetworkAclEntry(
            CreateNetworkAclEntryRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkInterfaceResult createNetworkInterface(
            CreateNetworkInterfaceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreatePlacementGroupResult createPlacementGroup(
            CreatePlacementGroupRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateReservedInstancesListingResult createReservedInstancesListing(
            CreateReservedInstancesListingRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteResult createRoute(CreateRouteRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteTableResult createRouteTable(
            CreateRouteTableRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSecurityGroupResult createSecurityGroup(
            CreateSecurityGroupRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSnapshotResult createSnapshot(CreateSnapshotRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSpotDatafeedSubscriptionResult createSpotDatafeedSubscription(
            CreateSpotDatafeedSubscriptionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSubnetResult createSubnet(CreateSubnetRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTagsResult createTags(CreateTagsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVolumeResult createVolume(CreateVolumeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcResult createVpc(CreateVpcRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcEndpointResult createVpcEndpoint(
            CreateVpcEndpointRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection(
            CreateVpcPeeringConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection() {
        return createVpcPeeringConnection(new CreateVpcPeeringConnectionRequest());
    }

    @Override
    public CreateVpnConnectionResult createVpnConnection(
            CreateVpnConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnConnectionRouteResult createVpnConnectionRoute(
            CreateVpnConnectionRouteRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnGatewayResult createVpnGateway(
            CreateVpnGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteCustomerGatewayResult deleteCustomerGateway(
            DeleteCustomerGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteDhcpOptionsResult deleteDhcpOptions(
            DeleteDhcpOptionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteFlowLogsResult deleteFlowLogs(DeleteFlowLogsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteInternetGatewayResult deleteInternetGateway(
            DeleteInternetGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteKeyPairResult deleteKeyPair(DeleteKeyPairRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNatGatewayResult deleteNatGateway(
            DeleteNatGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkAclResult deleteNetworkAcl(
            DeleteNetworkAclRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkAclEntryResult deleteNetworkAclEntry(
            DeleteNetworkAclEntryRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkInterfaceResult deleteNetworkInterface(
            DeleteNetworkInterfaceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeletePlacementGroupResult deletePlacementGroup(
            DeletePlacementGroupRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteRouteResult deleteRoute(DeleteRouteRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteRouteTableResult deleteRouteTable(
            DeleteRouteTableRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSecurityGroupResult deleteSecurityGroup(
            DeleteSecurityGroupRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSnapshotResult deleteSnapshot(DeleteSnapshotRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSpotDatafeedSubscriptionResult deleteSpotDatafeedSubscription(
            DeleteSpotDatafeedSubscriptionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSpotDatafeedSubscriptionResult deleteSpotDatafeedSubscription() {
        return deleteSpotDatafeedSubscription(new DeleteSpotDatafeedSubscriptionRequest());
    }

    @Override
    public DeleteSubnetResult deleteSubnet(DeleteSubnetRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTagsResult deleteTags(DeleteTagsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVolumeResult deleteVolume(DeleteVolumeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcResult deleteVpc(DeleteVpcRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcEndpointsResult deleteVpcEndpoints(
            DeleteVpcEndpointsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcPeeringConnectionResult deleteVpcPeeringConnection(
            DeleteVpcPeeringConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnConnectionResult deleteVpnConnection(
            DeleteVpnConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnConnectionRouteResult deleteVpnConnectionRoute(
            DeleteVpnConnectionRouteRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnGatewayResult deleteVpnGateway(
            DeleteVpnGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeregisterImageResult deregisterImage(DeregisterImageRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes(
            DescribeAccountAttributesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes() {
        return describeAccountAttributes(new DescribeAccountAttributesRequest());
    }

    @Override
    public DescribeAddressesResult describeAddresses(
            DescribeAddressesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAddressesResult describeAddresses() {
        return describeAddresses(new DescribeAddressesRequest());
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones(
            DescribeAvailabilityZonesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones() {
        return describeAvailabilityZones(new DescribeAvailabilityZonesRequest());
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks(
            DescribeBundleTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks() {
        return describeBundleTasks(new DescribeBundleTasksRequest());
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances(
            DescribeClassicLinkInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances() {
        return describeClassicLinkInstances(new DescribeClassicLinkInstancesRequest());
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks(
            DescribeConversionTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks() {
        return describeConversionTasks(new DescribeConversionTasksRequest());
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways(
            DescribeCustomerGatewaysRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways() {
        return describeCustomerGateways(new DescribeCustomerGatewaysRequest());
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions(
            DescribeDhcpOptionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions() {
        return describeDhcpOptions(new DescribeDhcpOptionsRequest());
    }

    @Override
    public DescribeExportTasksResult describeExportTasks(
            DescribeExportTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeExportTasksResult describeExportTasks() {
        return describeExportTasks(new DescribeExportTasksRequest());
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs(
            DescribeFlowLogsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs() {
        return describeFlowLogs(new DescribeFlowLogsRequest());
    }

    @Override
    public DescribeHostsResult describeHosts(DescribeHostsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeHostsResult describeHosts() {
        return describeHosts(new DescribeHostsRequest());
    }

    @Override
    public DescribeIdFormatResult describeIdFormat(
            DescribeIdFormatRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeIdFormatResult describeIdFormat() {
        return describeIdFormat(new DescribeIdFormatRequest());
    }

    @Override
    public DescribeIdentityIdFormatResult describeIdentityIdFormat(
            DescribeIdentityIdFormatRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImageAttributeResult describeImageAttribute(
            DescribeImageAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages(DescribeImagesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages() {
        return describeImages(new DescribeImagesRequest());
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks(
            DescribeImportImageTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks() {
        return describeImportImageTasks(new DescribeImportImageTasksRequest());
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks(
            DescribeImportSnapshotTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks() {
        return describeImportSnapshotTasks(new DescribeImportSnapshotTasksRequest());
    }

    @Override
    public DescribeInstanceAttributeResult describeInstanceAttribute(
            DescribeInstanceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus(
            DescribeInstanceStatusRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus() {
        return describeInstanceStatus(new DescribeInstanceStatusRequest());
    }

    @Override
    public DescribeInstancesResult describeInstances() {
        return describeInstances(new DescribeInstancesRequest());
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways(
            DescribeInternetGatewaysRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways() {
        return describeInternetGateways(new DescribeInternetGatewaysRequest());
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs(
            DescribeKeyPairsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs() {
        return describeKeyPairs(new DescribeKeyPairsRequest());
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses(
            DescribeMovingAddressesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses() {
        return describeMovingAddresses(new DescribeMovingAddressesRequest());
    }

    @Override
    public DescribeNatGatewaysResult describeNatGateways(
            DescribeNatGatewaysRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls(
            DescribeNetworkAclsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls() {
        return describeNetworkAcls(new DescribeNetworkAclsRequest());
    }

    @Override
    public DescribeNetworkInterfaceAttributeResult describeNetworkInterfaceAttribute(
            DescribeNetworkInterfaceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces(
            DescribeNetworkInterfacesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces() {
        return describeNetworkInterfaces(new DescribeNetworkInterfacesRequest());
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups(
            DescribePlacementGroupsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups() {
        return describePlacementGroups(new DescribePlacementGroupsRequest());
    }

    @Override
    public DescribePrefixListsResult describePrefixLists(
            DescribePrefixListsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrefixListsResult describePrefixLists() {
        return describePrefixLists(new DescribePrefixListsRequest());
    }

    @Override
    public DescribeRegionsResult describeRegions(DescribeRegionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRegionsResult describeRegions() {
        return describeRegions(new DescribeRegionsRequest());
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances(
            DescribeReservedInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances() {
        return describeReservedInstances(new DescribeReservedInstancesRequest());
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings(
            DescribeReservedInstancesListingsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings() {
        return describeReservedInstancesListings(new DescribeReservedInstancesListingsRequest());
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications(
            DescribeReservedInstancesModificationsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications() {
        return describeReservedInstancesModifications(new DescribeReservedInstancesModificationsRequest());
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings(
            DescribeReservedInstancesOfferingsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings() {
        return describeReservedInstancesOfferings(new DescribeReservedInstancesOfferingsRequest());
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables(
            DescribeRouteTablesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables() {
        return describeRouteTables(new DescribeRouteTablesRequest());
    }

    @Override
    public DescribeScheduledInstanceAvailabilityResult describeScheduledInstanceAvailability(
            DescribeScheduledInstanceAvailabilityRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeScheduledInstancesResult describeScheduledInstances(
            DescribeScheduledInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupReferencesResult describeSecurityGroupReferences(
            DescribeSecurityGroupReferencesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups(
            DescribeSecurityGroupsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups() {
        return describeSecurityGroups(new DescribeSecurityGroupsRequest());
    }

    @Override
    public DescribeSnapshotAttributeResult describeSnapshotAttribute(
            DescribeSnapshotAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots(
            DescribeSnapshotsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots() {
        return describeSnapshots(new DescribeSnapshotsRequest());
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription(
            DescribeSpotDatafeedSubscriptionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription() {
        return describeSpotDatafeedSubscription(new DescribeSpotDatafeedSubscriptionRequest());
    }

    @Override
    public DescribeSpotFleetInstancesResult describeSpotFleetInstances(
            DescribeSpotFleetInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestHistoryResult describeSpotFleetRequestHistory(
            DescribeSpotFleetRequestHistoryRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests(
            DescribeSpotFleetRequestsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests() {
        return describeSpotFleetRequests(new DescribeSpotFleetRequestsRequest());
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests(
            DescribeSpotInstanceRequestsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests() {
        return describeSpotInstanceRequests(new DescribeSpotInstanceRequestsRequest());
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(
            DescribeSpotPriceHistoryRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory() {
        return describeSpotPriceHistory(new DescribeSpotPriceHistoryRequest());
    }

    @Override
    public DescribeStaleSecurityGroupsResult describeStaleSecurityGroups(
            DescribeStaleSecurityGroupsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets(DescribeSubnetsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets() {
        return describeSubnets(new DescribeSubnetsRequest());
    }

    @Override
    public DescribeTagsResult describeTags(DescribeTagsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTagsResult describeTags() {
        return describeTags(new DescribeTagsRequest());
    }

    @Override
    public DescribeVolumeAttributeResult describeVolumeAttribute(
            DescribeVolumeAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus(
            DescribeVolumeStatusRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus() {
        return describeVolumeStatus(new DescribeVolumeStatusRequest());
    }

    @Override
    public DescribeVolumesResult describeVolumes(DescribeVolumesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesResult describeVolumes() {
        return describeVolumes(new DescribeVolumesRequest());
    }

    @Override
    public DescribeVpcAttributeResult describeVpcAttribute(
            DescribeVpcAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink(
            DescribeVpcClassicLinkRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink() {
        return describeVpcClassicLink(new DescribeVpcClassicLinkRequest());
    }

    @Override
    public DescribeVpcClassicLinkDnsSupportResult describeVpcClassicLinkDnsSupport(
            DescribeVpcClassicLinkDnsSupportRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices(
            DescribeVpcEndpointServicesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices() {
        return describeVpcEndpointServices(new DescribeVpcEndpointServicesRequest());
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints(
            DescribeVpcEndpointsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints() {
        return describeVpcEndpoints(new DescribeVpcEndpointsRequest());
    }

    @Override
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections(
            DescribeVpcPeeringConnectionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections() {
        return describeVpcPeeringConnections(new DescribeVpcPeeringConnectionsRequest());
    }

    @Override
    public DescribeVpcsResult describeVpcs(DescribeVpcsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcsResult describeVpcs() {
        return describeVpcs(new DescribeVpcsRequest());
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections(
            DescribeVpnConnectionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections() {
        return describeVpnConnections(new DescribeVpnConnectionsRequest());
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways(
            DescribeVpnGatewaysRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways() {
        return describeVpnGateways(new DescribeVpnGatewaysRequest());
    }

    @Override
    public DetachClassicLinkVpcResult detachClassicLinkVpc(
            DetachClassicLinkVpcRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachInternetGatewayResult detachInternetGateway(
            DetachInternetGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachNetworkInterfaceResult detachNetworkInterface(
            DetachNetworkInterfaceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachVolumeResult detachVolume(DetachVolumeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachVpnGatewayResult detachVpnGateway(
            DetachVpnGatewayRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVgwRoutePropagationResult disableVgwRoutePropagation(
            DisableVgwRoutePropagationRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkResult disableVpcClassicLink(
            DisableVpcClassicLinkRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkDnsSupportResult disableVpcClassicLinkDnsSupport(
            DisableVpcClassicLinkDnsSupportRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateAddressResult disassociateAddress(
            DisassociateAddressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateRouteTableResult disassociateRouteTable(
            DisassociateRouteTableRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVgwRoutePropagationResult enableVgwRoutePropagation(
            EnableVgwRoutePropagationRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVolumeIOResult enableVolumeIO(EnableVolumeIORequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkResult enableVpcClassicLink(
            EnableVpcClassicLinkRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkDnsSupportResult enableVpcClassicLinkDnsSupport(
            EnableVpcClassicLinkDnsSupportRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetConsoleOutputResult getConsoleOutput(
            GetConsoleOutputRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetConsoleScreenshotResult getConsoleScreenshot(
            GetConsoleScreenshotRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetPasswordDataResult getPasswordData(GetPasswordDataRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage(ImportImageRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage() {
        return importImage(new ImportImageRequest());
    }

    @Override
    public ImportInstanceResult importInstance(ImportInstanceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportKeyPairResult importKeyPair(ImportKeyPairRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot(ImportSnapshotRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot() {
        return importSnapshot(new ImportSnapshotRequest());
    }

    @Override
    public ImportVolumeResult importVolume(ImportVolumeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyHostsResult modifyHosts(ModifyHostsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyIdFormatResult modifyIdFormat(ModifyIdFormatRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyIdentityIdFormatResult modifyIdentityIdFormat(
            ModifyIdentityIdFormatRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyImageAttributeResult modifyImageAttribute(
            ModifyImageAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstanceAttributeResult modifyInstanceAttribute(
            ModifyInstanceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstancePlacementResult modifyInstancePlacement(
            ModifyInstancePlacementRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyNetworkInterfaceAttributeResult modifyNetworkInterfaceAttribute(
            ModifyNetworkInterfaceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyReservedInstancesResult modifyReservedInstances(
            ModifyReservedInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySnapshotAttributeResult modifySnapshotAttribute(
            ModifySnapshotAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySpotFleetRequestResult modifySpotFleetRequest(
            ModifySpotFleetRequestRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySubnetAttributeResult modifySubnetAttribute(
            ModifySubnetAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVolumeAttributeResult modifyVolumeAttribute(
            ModifyVolumeAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcAttributeResult modifyVpcAttribute(
            ModifyVpcAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointResult modifyVpcEndpoint(
            ModifyVpcEndpointRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcPeeringConnectionOptionsResult modifyVpcPeeringConnectionOptions(
            ModifyVpcPeeringConnectionOptionsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MonitorInstancesResult monitorInstances(
            MonitorInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MoveAddressToVpcResult moveAddressToVpc(
            MoveAddressToVpcRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseReservedInstancesOfferingResult purchaseReservedInstancesOffering(
            PurchaseReservedInstancesOfferingRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseScheduledInstancesResult purchaseScheduledInstances(
            PurchaseScheduledInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RebootInstancesResult rebootInstances(RebootInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RegisterImageResult registerImage(RegisterImageRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RejectVpcPeeringConnectionResult rejectVpcPeeringConnection(
            RejectVpcPeeringConnectionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReleaseAddressResult releaseAddress(ReleaseAddressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReleaseHostsResult releaseHosts(ReleaseHostsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceNetworkAclAssociationResult replaceNetworkAclAssociation(
            ReplaceNetworkAclAssociationRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceNetworkAclEntryResult replaceNetworkAclEntry(
            ReplaceNetworkAclEntryRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceRouteResult replaceRoute(ReplaceRouteRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceRouteTableAssociationResult replaceRouteTableAssociation(
            ReplaceRouteTableAssociationRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReportInstanceStatusResult reportInstanceStatus(
            ReportInstanceStatusRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotFleetResult requestSpotFleet(
            RequestSpotFleetRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotInstancesResult requestSpotInstances(
            RequestSpotInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetImageAttributeResult resetImageAttribute(
            ResetImageAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetInstanceAttributeResult resetInstanceAttribute(
            ResetInstanceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetNetworkInterfaceAttributeResult resetNetworkInterfaceAttribute(
            ResetNetworkInterfaceAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetSnapshotAttributeResult resetSnapshotAttribute(
            ResetSnapshotAttributeRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RestoreAddressToClassicResult restoreAddressToClassic(
            RestoreAddressToClassicRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupEgressResult revokeSecurityGroupEgress(
            RevokeSecurityGroupEgressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupIngressResult revokeSecurityGroupIngress(
            RevokeSecurityGroupIngressRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupIngressResult revokeSecurityGroupIngress() {
        return revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest());
    }

    @Override
    public RunInstancesResult runInstances(RunInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RunScheduledInstancesResult runScheduledInstances(
            RunScheduledInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StartInstancesResult startInstances(StartInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StopInstancesResult stopInstances(StopInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public TerminateInstancesResult terminateInstances(
            TerminateInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnassignPrivateIpAddressesResult unassignPrivateIpAddresses(
            UnassignPrivateIpAddressesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnmonitorInstancesResult unmonitorInstances(
            UnmonitorInstancesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public <X extends AmazonWebServiceRequest> DryRunResult<X> dryRun(
            DryRunSupportedRequest<X> request) throws AmazonServiceException,
            AmazonClientException {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void shutdown() {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public com.amazonaws.ResponseMetadata getCachedResponseMetadata(
            com.amazonaws.AmazonWebServiceRequest request) {
        throw new java.lang.UnsupportedOperationException("Not supported in mock");
    }
}
