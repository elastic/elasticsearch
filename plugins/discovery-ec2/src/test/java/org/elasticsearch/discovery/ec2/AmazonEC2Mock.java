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
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.AssociateAddressResult;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.AssociateRouteTableRequest;
import com.amazonaws.services.ec2.model.AssociateRouteTableResult;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcRequest;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcResult;
import com.amazonaws.services.ec2.model.AttachInternetGatewayRequest;
import com.amazonaws.services.ec2.model.AttachNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.AttachNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.AttachVolumeResult;
import com.amazonaws.services.ec2.model.AttachVpnGatewayRequest;
import com.amazonaws.services.ec2.model.AttachVpnGatewayResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.BundleInstanceRequest;
import com.amazonaws.services.ec2.model.BundleInstanceResult;
import com.amazonaws.services.ec2.model.CancelBundleTaskRequest;
import com.amazonaws.services.ec2.model.CancelBundleTaskResult;
import com.amazonaws.services.ec2.model.CancelConversionTaskRequest;
import com.amazonaws.services.ec2.model.CancelExportTaskRequest;
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
import com.amazonaws.services.ec2.model.CreateNetworkAclRequest;
import com.amazonaws.services.ec2.model.CreateNetworkAclResult;
import com.amazonaws.services.ec2.model.CreateNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.CreateNetworkInterfaceResult;
import com.amazonaws.services.ec2.model.CreatePlacementGroupRequest;
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
import com.amazonaws.services.ec2.model.CreateVpnGatewayRequest;
import com.amazonaws.services.ec2.model.CreateVpnGatewayResult;
import com.amazonaws.services.ec2.model.DeleteCustomerGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DeleteFlowLogsRequest;
import com.amazonaws.services.ec2.model.DeleteFlowLogsResult;
import com.amazonaws.services.ec2.model.DeleteInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteNatGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteNatGatewayResult;
import com.amazonaws.services.ec2.model.DeleteNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DeletePlacementGroupRequest;
import com.amazonaws.services.ec2.model.DeleteRouteRequest;
import com.amazonaws.services.ec2.model.DeleteRouteTableRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSnapshotRequest;
import com.amazonaws.services.ec2.model.DeleteSpotDatafeedSubscriptionRequest;
import com.amazonaws.services.ec2.model.DeleteSubnetRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointsResult;
import com.amazonaws.services.ec2.model.DeleteVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.DeleteVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.DeleteVpcRequest;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionRequest;
import com.amazonaws.services.ec2.model.DeleteVpnConnectionRouteRequest;
import com.amazonaws.services.ec2.model.DeleteVpnGatewayRequest;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
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
import com.amazonaws.services.ec2.model.DetachNetworkInterfaceRequest;
import com.amazonaws.services.ec2.model.DetachVolumeRequest;
import com.amazonaws.services.ec2.model.DetachVolumeResult;
import com.amazonaws.services.ec2.model.DetachVpnGatewayRequest;
import com.amazonaws.services.ec2.model.DisableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.DisassociateRouteTableRequest;
import com.amazonaws.services.ec2.model.DryRunResult;
import com.amazonaws.services.ec2.model.DryRunSupportedRequest;
import com.amazonaws.services.ec2.model.EnableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.EnableVolumeIORequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GetConsoleOutputRequest;
import com.amazonaws.services.ec2.model.GetConsoleOutputResult;
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
import com.amazonaws.services.ec2.model.ModifyImageAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementRequest;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementResult;
import com.amazonaws.services.ec2.model.ModifyNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyReservedInstancesRequest;
import com.amazonaws.services.ec2.model.ModifyReservedInstancesResult;
import com.amazonaws.services.ec2.model.ModifySnapshotAttributeRequest;
import com.amazonaws.services.ec2.model.ModifySpotFleetRequestRequest;
import com.amazonaws.services.ec2.model.ModifySpotFleetRequestResult;
import com.amazonaws.services.ec2.model.ModifySubnetAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVolumeAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVpcAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointResult;
import com.amazonaws.services.ec2.model.MonitorInstancesRequest;
import com.amazonaws.services.ec2.model.MonitorInstancesResult;
import com.amazonaws.services.ec2.model.MoveAddressToVpcRequest;
import com.amazonaws.services.ec2.model.MoveAddressToVpcResult;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingRequest;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingResult;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesResult;
import com.amazonaws.services.ec2.model.RebootInstancesRequest;
import com.amazonaws.services.ec2.model.RegisterImageRequest;
import com.amazonaws.services.ec2.model.RegisterImageResult;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.ReleaseAddressRequest;
import com.amazonaws.services.ec2.model.ReleaseHostsRequest;
import com.amazonaws.services.ec2.model.ReleaseHostsResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationResult;
import com.amazonaws.services.ec2.model.ReportInstanceStatusRequest;
import com.amazonaws.services.ec2.model.RequestSpotFleetRequest;
import com.amazonaws.services.ec2.model.RequestSpotFleetResult;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResetImageAttributeRequest;
import com.amazonaws.services.ec2.model.ResetInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.ResetNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.ResetSnapshotAttributeRequest;
import com.amazonaws.services.ec2.model.RestoreAddressToClassicRequest;
import com.amazonaws.services.ec2.model.RestoreAddressToClassicResult;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
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
import com.amazonaws.services.ec2.model.UnmonitorInstancesRequest;
import com.amazonaws.services.ec2.model.UnmonitorInstancesResult;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonEC2Mock implements AmazonEC2 {

    private static final ESLogger logger = ESLoggerFactory.getLogger(AmazonEC2Mock.class.getName());

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
    public void setEndpoint(String endpoint) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void rebootInstances(RebootInstancesRequest rebootInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances(DescribeReservedInstancesRequest describeReservedInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateFlowLogsResult createFlowLogs(CreateFlowLogsRequest createFlowLogsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones(DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RestoreAddressToClassicResult restoreAddressToClassic(RestoreAddressToClassicRequest restoreAddressToClassicRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachVolumeResult detachVolume(DetachVolumeRequest detachVolumeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteKeyPair(DeleteKeyPairRequest deleteKeyPairRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNatGatewayResult deleteNatGateway(DeleteNatGatewayRequest deleteNatGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnmonitorInstancesResult unmonitorInstances(UnmonitorInstancesRequest unmonitorInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVpnGatewayResult attachVpnGateway(AttachVpnGatewayRequest attachVpnGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateImageResult createImage(CreateImageRequest createImageRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteSecurityGroup(DeleteSecurityGroupRequest deleteSecurityGroupRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInstanceExportTaskResult createInstanceExportTask(CreateInstanceExportTaskRequest createInstanceExportTaskRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void authorizeSecurityGroupEgress(AuthorizeSecurityGroupEgressRequest authorizeSecurityGroupEgressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void associateDhcpOptions(AssociateDhcpOptionsRequest associateDhcpOptionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetPasswordDataResult getPasswordData(GetPasswordDataRequest getPasswordDataRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StopInstancesResult stopInstances(StopInstancesRequest stopInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportKeyPairResult importKeyPair(ImportKeyPairRequest importKeyPairRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteNetworkInterface(DeleteNetworkInterfaceRequest deleteNetworkInterfaceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyVpcAttribute(ModifyVpcAttributeRequest modifyVpcAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetInstancesResult describeSpotFleetInstances(DescribeSpotFleetInstancesRequest describeSpotFleetInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSecurityGroupResult createSecurityGroup(CreateSecurityGroupRequest createSecurityGroupRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces(DescribeNetworkInterfacesRequest describeNetworkInterfacesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRegionsResult describeRegions(DescribeRegionsRequest describeRegionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateDhcpOptionsResult createDhcpOptions(CreateDhcpOptionsRequest createDhcpOptionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateReservedInstancesListingResult createReservedInstancesListing(CreateReservedInstancesListingRequest createReservedInstancesListingRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcEndpointsResult deleteVpcEndpoints(DeleteVpcEndpointsRequest deleteVpcEndpointsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void resetSnapshotAttribute(ResetSnapshotAttributeRequest resetSnapshotAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteRoute(DeleteRouteRequest deleteRouteRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways(DescribeInternetGatewaysRequest describeInternetGatewaysRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportVolumeResult importVolume(ImportVolumeRequest importVolumeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyHostsResult modifyHosts(ModifyHostsRequest modifyHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyIdFormat(ModifyIdFormatRequest modifyIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups(DescribeSecurityGroupsRequest describeSecurityGroupsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RejectVpcPeeringConnectionResult rejectVpcPeeringConnection(RejectVpcPeeringConnectionRequest rejectVpcPeeringConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteFlowLogsResult deleteFlowLogs(DeleteFlowLogsRequest deleteFlowLogsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void detachVpnGateway(DetachVpnGatewayRequest detachVpnGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deregisterImage(DeregisterImageRequest deregisterImageRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription(DescribeSpotDatafeedSubscriptionRequest describeSpotDatafeedSubscriptionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteTags(DeleteTagsRequest deleteTagsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteSubnet(DeleteSubnetRequest deleteSubnetRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes(DescribeAccountAttributesRequest describeAccountAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachClassicLinkVpcResult attachClassicLinkVpc(AttachClassicLinkVpcRequest attachClassicLinkVpcRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnGatewayResult createVpnGateway(CreateVpnGatewayRequest createVpnGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void enableVolumeIO(EnableVolumeIORequest enableVolumeIORequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MoveAddressToVpcResult moveAddressToVpc(MoveAddressToVpcRequest moveAddressToVpcRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteVpnGateway(DeleteVpnGatewayRequest deleteVpnGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVolumeResult attachVolume(AttachVolumeRequest attachVolumeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus(DescribeVolumeStatusRequest describeVolumeStatusRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks(DescribeImportSnapshotTasksRequest describeImportSnapshotTasksRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections(DescribeVpnConnectionsRequest describeVpnConnectionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void resetImageAttribute(ResetImageAttributeRequest resetImageAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void enableVgwRoutePropagation(EnableVgwRoutePropagationRequest enableVgwRoutePropagationRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSnapshotResult createSnapshot(CreateSnapshotRequest createSnapshotRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteVolume(DeleteVolumeRequest deleteVolumeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkInterfaceResult createNetworkInterface(CreateNetworkInterfaceRequest createNetworkInterfaceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyReservedInstancesResult modifyReservedInstances(ModifyReservedInstancesRequest modifyReservedInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotFleetRequestsResult cancelSpotFleetRequests(CancelSpotFleetRequestsRequest cancelSpotFleetRequestsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void unassignPrivateIpAddresses(UnassignPrivateIpAddressesRequest unassignPrivateIpAddressesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcsResult describeVpcs(DescribeVpcsRequest describeVpcsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void cancelConversionTask(CancelConversionTaskRequest cancelConversionTaskRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateAddressResult associateAddress(AssociateAddressRequest associateAddressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteCustomerGateway(DeleteCustomerGatewayRequest deleteCustomerGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void createNetworkAclEntry(CreateNetworkAclEntryRequest createNetworkAclEntryRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection(AcceptVpcPeeringConnectionRequest acceptVpcPeeringConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeExportTasksResult describeExportTasks(DescribeExportTasksRequest describeExportTasksRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void detachInternetGateway(DetachInternetGatewayRequest detachInternetGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection(CreateVpcPeeringConnectionRequest createVpcPeeringConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteTableResult createRouteTable(CreateRouteTableRequest createRouteTableRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask(CancelImportTaskRequest cancelImportTaskRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesResult describeVolumes(DescribeVolumesRequest describeVolumesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings(DescribeReservedInstancesListingsRequest describeReservedInstancesListingsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void reportInstanceStatus(ReportInstanceStatusRequest reportInstanceStatusRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables(DescribeRouteTablesRequest describeRouteTablesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions(DescribeDhcpOptionsRequest describeDhcpOptionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MonitorInstancesResult monitorInstances(MonitorInstancesRequest monitorInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrefixListsResult describePrefixLists(DescribePrefixListsRequest describePrefixListsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotFleetResult requestSpotFleet(RequestSpotFleetRequest requestSpotFleetRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks(DescribeImportImageTasksRequest describeImportImageTasksRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls(DescribeNetworkAclsRequest describeNetworkAclsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks(DescribeBundleTasksRequest describeBundleTasksRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportInstanceResult importInstance(ImportInstanceRequest importInstanceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void revokeSecurityGroupIngress(RevokeSecurityGroupIngressRequest revokeSecurityGroupIngressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcPeeringConnectionResult deleteVpcPeeringConnection(DeleteVpcPeeringConnectionRequest deleteVpcPeeringConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetConsoleOutputResult getConsoleOutput(GetConsoleOutputRequest getConsoleOutputRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway(CreateInternetGatewayRequest createInternetGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteVpnConnectionRoute(DeleteVpnConnectionRouteRequest deleteVpnConnectionRouteRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void detachNetworkInterface(DetachNetworkInterfaceRequest detachNetworkInterfaceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyImageAttribute(ModifyImageAttributeRequest modifyImageAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateCustomerGatewayResult createCustomerGateway(CreateCustomerGatewayRequest createCustomerGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSpotDatafeedSubscriptionResult createSpotDatafeedSubscription(CreateSpotDatafeedSubscriptionRequest createSpotDatafeedSubscriptionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void attachInternetGateway(AttachInternetGatewayRequest attachInternetGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteVpnConnection(DeleteVpnConnectionRequest deleteVpnConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses(DescribeMovingAddressesRequest describeMovingAddressesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks(DescribeConversionTasksRequest describeConversionTasksRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnConnectionResult createVpnConnection(CreateVpnConnectionRequest createVpnConnectionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage(ImportImageRequest importImageRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkResult disableVpcClassicLink(DisableVpcClassicLinkRequest disableVpcClassicLinkRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkDnsSupportResult disableVpcClassicLinkDnsSupport(DisableVpcClassicLinkDnsSupportRequest disableVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceAttributeResult describeInstanceAttribute(DescribeInstanceAttributeRequest describeInstanceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs(DescribeFlowLogsRequest describeFlowLogsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections(DescribeVpcPeeringConnectionsRequest describeVpcPeeringConnectionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups(DescribePlacementGroupsRequest describePlacementGroupsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RunInstancesResult runInstances(RunInstancesRequest runInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RunScheduledInstancesResult runScheduledInstances(RunScheduledInstancesRequest runScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets(DescribeSubnetsRequest describeSubnetsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateRouteTableResult associateRouteTable(AssociateRouteTableRequest associateRouteTableRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyVolumeAttribute(ModifyVolumeAttributeRequest modifyVolumeAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteNetworkAcl(DeleteNetworkAclRequest deleteNetworkAclRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages(DescribeImagesRequest describeImagesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StartInstancesResult startInstances(StartInstancesRequest startInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyInstanceAttribute(ModifyInstanceAttributeRequest modifyInstanceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstancePlacementResult modifyInstancePlacement(ModifyInstancePlacementRequest modifyInstancePlacementRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelReservedInstancesListingResult cancelReservedInstancesListing(CancelReservedInstancesListingRequest cancelReservedInstancesListingRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteDhcpOptions(DeleteDhcpOptionsRequest deleteDhcpOptionsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void authorizeSecurityGroupIngress(AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests(DescribeSpotInstanceRequestsRequest describeSpotInstanceRequestsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcResult createVpc(CreateVpcRequest createVpcRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways(DescribeCustomerGatewaysRequest describeCustomerGatewaysRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void cancelExportTask(CancelExportTaskRequest cancelExportTaskRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteResult createRoute(CreateRouteRequest createRouteRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcEndpointResult createVpcEndpoint(CreateVpcEndpointRequest createVpcEndpointRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopyImageResult copyImage(CopyImageRequest copyImageRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink(DescribeVpcClassicLinkRequest describeVpcClassicLinkRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifyNetworkInterfaceAttribute(ModifyNetworkInterfaceAttributeRequest modifyNetworkInterfaceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteRouteTable(DeleteRouteTableRequest deleteRouteTableRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfaceAttributeResult describeNetworkInterfaceAttribute(DescribeNetworkInterfaceAttributeRequest describeNetworkInterfaceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances(DescribeClassicLinkInstancesRequest describeClassicLinkInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotInstancesResult requestSpotInstances(RequestSpotInstancesRequest requestSpotInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void createTags(CreateTagsRequest createTagsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeAttributeResult describeVolumeAttribute(DescribeVolumeAttributeRequest describeVolumeAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachNetworkInterfaceResult attachNetworkInterface(AttachNetworkInterfaceRequest attachNetworkInterfaceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void replaceRoute(ReplaceRouteRequest replaceRouteRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTagsResult describeTags(DescribeTagsRequest describeTagsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelBundleTaskResult cancelBundleTask(CancelBundleTaskRequest cancelBundleTaskRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void disableVgwRoutePropagation(DisableVgwRoutePropagationRequest disableVgwRoutePropagationRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot(ImportSnapshotRequest importSnapshotRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotInstanceRequestsResult cancelSpotInstanceRequests(CancelSpotInstanceRequestsRequest cancelSpotInstanceRequestsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests(DescribeSpotFleetRequestsRequest describeSpotFleetRequestsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseReservedInstancesOfferingResult purchaseReservedInstancesOffering(PurchaseReservedInstancesOfferingRequest purchaseReservedInstancesOfferingRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseScheduledInstancesResult purchaseScheduledInstances(PurchaseScheduledInstancesRequest purchaseScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifySnapshotAttribute(ModifySnapshotAttributeRequest modifySnapshotAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications(DescribeReservedInstancesModificationsRequest describeReservedInstancesModificationsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public TerminateInstancesResult terminateInstances(TerminateInstancesRequest terminateInstancesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointResult modifyVpcEndpoint(ModifyVpcEndpointRequest modifyVpcEndpointRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteSpotDatafeedSubscription(DeleteSpotDatafeedSubscriptionRequest deleteSpotDatafeedSubscriptionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteInternetGateway(DeleteInternetGatewayRequest deleteInternetGatewayRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotAttributeResult describeSnapshotAttribute(DescribeSnapshotAttributeRequest describeSnapshotAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceRouteTableAssociationResult replaceRouteTableAssociation(ReplaceRouteTableAssociationRequest replaceRouteTableAssociationRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAddressesResult describeAddresses(DescribeAddressesRequest describeAddressesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImageAttributeResult describeImageAttribute(DescribeImageAttributeRequest describeImageAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs(DescribeKeyPairsRequest describeKeyPairsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ConfirmProductInstanceResult confirmProductInstance(ConfirmProductInstanceRequest confirmProductInstanceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void disassociateRouteTable(DisassociateRouteTableRequest disassociateRouteTableRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcAttributeResult describeVpcAttribute(DescribeVpcAttributeRequest describeVpcAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void revokeSecurityGroupEgress(RevokeSecurityGroupEgressRequest revokeSecurityGroupEgressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteNetworkAclEntry(DeleteNetworkAclEntryRequest deleteNetworkAclEntryRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVolumeResult createVolume(CreateVolumeRequest createVolumeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus(DescribeInstanceStatusRequest describeInstanceStatusRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways(DescribeVpnGatewaysRequest describeVpnGatewaysRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSubnetResult createSubnet(CreateSubnetRequest createSubnetRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings(DescribeReservedInstancesOfferingsRequest describeReservedInstancesOfferingsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void assignPrivateIpAddresses(AssignPrivateIpAddressesRequest assignPrivateIpAddressesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestHistoryResult describeSpotFleetRequestHistory(DescribeSpotFleetRequestHistoryRequest describeSpotFleetRequestHistoryRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteSnapshot(DeleteSnapshotRequest deleteSnapshotRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceNetworkAclAssociationResult replaceNetworkAclAssociation(ReplaceNetworkAclAssociationRequest replaceNetworkAclAssociationRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void disassociateAddress(DisassociateAddressRequest disassociateAddressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void createPlacementGroup(CreatePlacementGroupRequest createPlacementGroupRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public BundleInstanceResult bundleInstance(BundleInstanceRequest bundleInstanceRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deletePlacementGroup(DeletePlacementGroupRequest deletePlacementGroupRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void modifySubnetAttribute(ModifySubnetAttributeRequest modifySubnetAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteVpc(DeleteVpcRequest deleteVpcRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopySnapshotResult copySnapshot(CopySnapshotRequest copySnapshotRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices(DescribeVpcEndpointServicesRequest describeVpcEndpointServicesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateAddressResult allocateAddress(AllocateAddressRequest allocateAddressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void releaseAddress(ReleaseAddressRequest releaseAddressRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReleaseHostsResult releaseHosts(ReleaseHostsRequest releaseHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void resetInstanceAttribute(ResetInstanceAttributeRequest resetInstanceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateKeyPairResult createKeyPair(CreateKeyPairRequest createKeyPairRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNatGatewayResult createNatGateway(CreateNatGatewayRequest createNatGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void replaceNetworkAclEntry(ReplaceNetworkAclEntryRequest replaceNetworkAclEntryRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots(DescribeSnapshotsRequest describeSnapshotsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkAclResult createNetworkAcl(CreateNetworkAclRequest createNetworkAclRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RegisterImageResult registerImage(RegisterImageRequest registerImageRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void resetNetworkInterfaceAttribute(ResetNetworkInterfaceAttributeRequest resetNetworkInterfaceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkResult enableVpcClassicLink(EnableVpcClassicLinkRequest enableVpcClassicLinkRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkDnsSupportResult enableVpcClassicLinkDnsSupport(EnableVpcClassicLinkDnsSupportRequest enableVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void createVpnConnectionRoute(CreateVpnConnectionRouteRequest createVpnConnectionRouteRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints(DescribeVpcEndpointsRequest describeVpcEndpointsRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachClassicLinkVpcResult detachClassicLinkVpc(DetachClassicLinkVpcRequest detachClassicLinkVpcRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRegionsResult describeRegions() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcsResult describeVpcs() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeExportTasksResult describeExportTasks() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesResult describeVolumes() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeScheduledInstanceAvailabilityResult describeScheduledInstanceAvailability(DescribeScheduledInstanceAvailabilityRequest describeScheduledInstanceAvailabilityRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeScheduledInstancesResult describeScheduledInstances(DescribeScheduledInstancesRequest describeScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrefixListsResult describePrefixLists() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void revokeSecurityGroupIngress() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNatGatewaysResult describeNatGateways(DescribeNatGatewaysRequest describeNatGatewaysRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeHostsResult describeHosts(DescribeHostsRequest describeHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeHostsResult describeHosts() {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeIdFormatResult describeIdFormat(DescribeIdFormatRequest describeIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeIdFormatResult describeIdFormat() {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstancesResult describeInstances() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkDnsSupportResult describeVpcClassicLinkDnsSupport(DescribeVpcClassicLinkDnsSupportRequest describeVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTagsResult describeTags() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void deleteSpotDatafeedSubscription() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAddressesResult describeAddresses() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateAddressResult allocateAddress() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateHostsResult allocateHosts(AllocateHostsRequest allocateHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints() throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public <X extends AmazonWebServiceRequest> DryRunResult<X> dryRun(DryRunSupportedRequest<X> request) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySpotFleetRequestResult modifySpotFleetRequest(ModifySpotFleetRequestRequest modifySpotFleetRequestRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }
}
