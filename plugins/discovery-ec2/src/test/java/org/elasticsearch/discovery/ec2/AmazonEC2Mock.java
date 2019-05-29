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
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AcceptReservedInstancesExchangeQuoteRequest;
import com.amazonaws.services.ec2.model.AcceptReservedInstancesExchangeQuoteResult;
import com.amazonaws.services.ec2.model.AcceptTransitGatewayVpcAttachmentRequest;
import com.amazonaws.services.ec2.model.AcceptTransitGatewayVpcAttachmentResult;
import com.amazonaws.services.ec2.model.AcceptVpcEndpointConnectionsRequest;
import com.amazonaws.services.ec2.model.AcceptVpcEndpointConnectionsResult;
import com.amazonaws.services.ec2.model.AcceptVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.AcceptVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.AdvertiseByoipCidrRequest;
import com.amazonaws.services.ec2.model.AdvertiseByoipCidrResult;
import com.amazonaws.services.ec2.model.AllocateAddressRequest;
import com.amazonaws.services.ec2.model.AllocateAddressResult;
import com.amazonaws.services.ec2.model.AllocateHostsRequest;
import com.amazonaws.services.ec2.model.AllocateHostsResult;
import com.amazonaws.services.ec2.model.ApplySecurityGroupsToClientVpnTargetNetworkRequest;
import com.amazonaws.services.ec2.model.ApplySecurityGroupsToClientVpnTargetNetworkResult;
import com.amazonaws.services.ec2.model.AssignIpv6AddressesRequest;
import com.amazonaws.services.ec2.model.AssignIpv6AddressesResult;
import com.amazonaws.services.ec2.model.AssignPrivateIpAddressesRequest;
import com.amazonaws.services.ec2.model.AssignPrivateIpAddressesResult;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.AssociateAddressResult;
import com.amazonaws.services.ec2.model.AssociateClientVpnTargetNetworkRequest;
import com.amazonaws.services.ec2.model.AssociateClientVpnTargetNetworkResult;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.AssociateDhcpOptionsResult;
import com.amazonaws.services.ec2.model.AssociateIamInstanceProfileRequest;
import com.amazonaws.services.ec2.model.AssociateIamInstanceProfileResult;
import com.amazonaws.services.ec2.model.AssociateRouteTableRequest;
import com.amazonaws.services.ec2.model.AssociateRouteTableResult;
import com.amazonaws.services.ec2.model.AssociateSubnetCidrBlockRequest;
import com.amazonaws.services.ec2.model.AssociateSubnetCidrBlockResult;
import com.amazonaws.services.ec2.model.AssociateTransitGatewayRouteTableRequest;
import com.amazonaws.services.ec2.model.AssociateTransitGatewayRouteTableResult;
import com.amazonaws.services.ec2.model.AssociateVpcCidrBlockRequest;
import com.amazonaws.services.ec2.model.AssociateVpcCidrBlockResult;
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
import com.amazonaws.services.ec2.model.AuthorizeClientVpnIngressRequest;
import com.amazonaws.services.ec2.model.AuthorizeClientVpnIngressResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressResult;
import com.amazonaws.services.ec2.model.BundleInstanceRequest;
import com.amazonaws.services.ec2.model.BundleInstanceResult;
import com.amazonaws.services.ec2.model.CancelBundleTaskRequest;
import com.amazonaws.services.ec2.model.CancelBundleTaskResult;
import com.amazonaws.services.ec2.model.CancelCapacityReservationRequest;
import com.amazonaws.services.ec2.model.CancelCapacityReservationResult;
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
import com.amazonaws.services.ec2.model.CopyFpgaImageRequest;
import com.amazonaws.services.ec2.model.CopyFpgaImageResult;
import com.amazonaws.services.ec2.model.CopyImageRequest;
import com.amazonaws.services.ec2.model.CopyImageResult;
import com.amazonaws.services.ec2.model.CopySnapshotRequest;
import com.amazonaws.services.ec2.model.CopySnapshotResult;
import com.amazonaws.services.ec2.model.CreateCapacityReservationRequest;
import com.amazonaws.services.ec2.model.CreateCapacityReservationResult;
import com.amazonaws.services.ec2.model.CreateClientVpnEndpointRequest;
import com.amazonaws.services.ec2.model.CreateClientVpnEndpointResult;
import com.amazonaws.services.ec2.model.CreateClientVpnRouteRequest;
import com.amazonaws.services.ec2.model.CreateClientVpnRouteResult;
import com.amazonaws.services.ec2.model.CreateCustomerGatewayRequest;
import com.amazonaws.services.ec2.model.CreateCustomerGatewayResult;
import com.amazonaws.services.ec2.model.CreateDefaultSubnetRequest;
import com.amazonaws.services.ec2.model.CreateDefaultSubnetResult;
import com.amazonaws.services.ec2.model.CreateDefaultVpcRequest;
import com.amazonaws.services.ec2.model.CreateDefaultVpcResult;
import com.amazonaws.services.ec2.model.CreateDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.CreateDhcpOptionsResult;
import com.amazonaws.services.ec2.model.CreateEgressOnlyInternetGatewayRequest;
import com.amazonaws.services.ec2.model.CreateEgressOnlyInternetGatewayResult;
import com.amazonaws.services.ec2.model.CreateFleetRequest;
import com.amazonaws.services.ec2.model.CreateFleetResult;
import com.amazonaws.services.ec2.model.CreateFlowLogsRequest;
import com.amazonaws.services.ec2.model.CreateFlowLogsResult;
import com.amazonaws.services.ec2.model.CreateFpgaImageRequest;
import com.amazonaws.services.ec2.model.CreateFpgaImageResult;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.CreateInstanceExportTaskRequest;
import com.amazonaws.services.ec2.model.CreateInstanceExportTaskResult;
import com.amazonaws.services.ec2.model.CreateInternetGatewayRequest;
import com.amazonaws.services.ec2.model.CreateInternetGatewayResult;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateLaunchTemplateRequest;
import com.amazonaws.services.ec2.model.CreateLaunchTemplateResult;
import com.amazonaws.services.ec2.model.CreateLaunchTemplateVersionRequest;
import com.amazonaws.services.ec2.model.CreateLaunchTemplateVersionResult;
import com.amazonaws.services.ec2.model.CreateNatGatewayRequest;
import com.amazonaws.services.ec2.model.CreateNatGatewayResult;
import com.amazonaws.services.ec2.model.CreateNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.CreateNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.CreateNetworkAclRequest;
import com.amazonaws.services.ec2.model.CreateNetworkAclResult;
import com.amazonaws.services.ec2.model.CreateNetworkInterfacePermissionRequest;
import com.amazonaws.services.ec2.model.CreateNetworkInterfacePermissionResult;
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
import com.amazonaws.services.ec2.model.CreateTransitGatewayRequest;
import com.amazonaws.services.ec2.model.CreateTransitGatewayResult;
import com.amazonaws.services.ec2.model.CreateTransitGatewayRouteRequest;
import com.amazonaws.services.ec2.model.CreateTransitGatewayRouteResult;
import com.amazonaws.services.ec2.model.CreateTransitGatewayRouteTableRequest;
import com.amazonaws.services.ec2.model.CreateTransitGatewayRouteTableResult;
import com.amazonaws.services.ec2.model.CreateTransitGatewayVpcAttachmentRequest;
import com.amazonaws.services.ec2.model.CreateTransitGatewayVpcAttachmentResult;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.CreateVpcEndpointConnectionNotificationRequest;
import com.amazonaws.services.ec2.model.CreateVpcEndpointConnectionNotificationResult;
import com.amazonaws.services.ec2.model.CreateVpcEndpointRequest;
import com.amazonaws.services.ec2.model.CreateVpcEndpointResult;
import com.amazonaws.services.ec2.model.CreateVpcEndpointServiceConfigurationRequest;
import com.amazonaws.services.ec2.model.CreateVpcEndpointServiceConfigurationResult;
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
import com.amazonaws.services.ec2.model.DeleteClientVpnEndpointRequest;
import com.amazonaws.services.ec2.model.DeleteClientVpnEndpointResult;
import com.amazonaws.services.ec2.model.DeleteClientVpnRouteRequest;
import com.amazonaws.services.ec2.model.DeleteClientVpnRouteResult;
import com.amazonaws.services.ec2.model.DeleteCustomerGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteCustomerGatewayResult;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DeleteDhcpOptionsResult;
import com.amazonaws.services.ec2.model.DeleteEgressOnlyInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteEgressOnlyInternetGatewayResult;
import com.amazonaws.services.ec2.model.DeleteFleetsRequest;
import com.amazonaws.services.ec2.model.DeleteFleetsResult;
import com.amazonaws.services.ec2.model.DeleteFlowLogsRequest;
import com.amazonaws.services.ec2.model.DeleteFlowLogsResult;
import com.amazonaws.services.ec2.model.DeleteFpgaImageRequest;
import com.amazonaws.services.ec2.model.DeleteFpgaImageResult;
import com.amazonaws.services.ec2.model.DeleteInternetGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteInternetGatewayResult;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairResult;
import com.amazonaws.services.ec2.model.DeleteLaunchTemplateRequest;
import com.amazonaws.services.ec2.model.DeleteLaunchTemplateResult;
import com.amazonaws.services.ec2.model.DeleteLaunchTemplateVersionsRequest;
import com.amazonaws.services.ec2.model.DeleteLaunchTemplateVersionsResult;
import com.amazonaws.services.ec2.model.DeleteNatGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteNatGatewayResult;
import com.amazonaws.services.ec2.model.DeleteNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.DeleteNetworkAclRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkAclResult;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfacePermissionRequest;
import com.amazonaws.services.ec2.model.DeleteNetworkInterfacePermissionResult;
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
import com.amazonaws.services.ec2.model.DeleteTransitGatewayRequest;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayResult;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayRouteRequest;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayRouteResult;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayRouteTableRequest;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayRouteTableResult;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayVpcAttachmentRequest;
import com.amazonaws.services.ec2.model.DeleteTransitGatewayVpcAttachmentResult;
import com.amazonaws.services.ec2.model.DeleteVolumeRequest;
import com.amazonaws.services.ec2.model.DeleteVolumeResult;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointConnectionNotificationsRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointConnectionNotificationsResult;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointServiceConfigurationsRequest;
import com.amazonaws.services.ec2.model.DeleteVpcEndpointServiceConfigurationsResult;
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
import com.amazonaws.services.ec2.model.DeprovisionByoipCidrRequest;
import com.amazonaws.services.ec2.model.DeprovisionByoipCidrResult;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DeregisterImageResult;
import com.amazonaws.services.ec2.model.DescribeAccountAttributesRequest;
import com.amazonaws.services.ec2.model.DescribeAccountAttributesResult;
import com.amazonaws.services.ec2.model.DescribeAddressesRequest;
import com.amazonaws.services.ec2.model.DescribeAddressesResult;
import com.amazonaws.services.ec2.model.DescribeAggregateIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribeAggregateIdFormatResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeBundleTasksRequest;
import com.amazonaws.services.ec2.model.DescribeBundleTasksResult;
import com.amazonaws.services.ec2.model.DescribeByoipCidrsRequest;
import com.amazonaws.services.ec2.model.DescribeByoipCidrsResult;
import com.amazonaws.services.ec2.model.DescribeCapacityReservationsRequest;
import com.amazonaws.services.ec2.model.DescribeCapacityReservationsResult;
import com.amazonaws.services.ec2.model.DescribeClassicLinkInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeClassicLinkInstancesResult;
import com.amazonaws.services.ec2.model.DescribeClientVpnAuthorizationRulesRequest;
import com.amazonaws.services.ec2.model.DescribeClientVpnAuthorizationRulesResult;
import com.amazonaws.services.ec2.model.DescribeClientVpnConnectionsRequest;
import com.amazonaws.services.ec2.model.DescribeClientVpnConnectionsResult;
import com.amazonaws.services.ec2.model.DescribeClientVpnEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeClientVpnEndpointsResult;
import com.amazonaws.services.ec2.model.DescribeClientVpnRoutesRequest;
import com.amazonaws.services.ec2.model.DescribeClientVpnRoutesResult;
import com.amazonaws.services.ec2.model.DescribeClientVpnTargetNetworksRequest;
import com.amazonaws.services.ec2.model.DescribeClientVpnTargetNetworksResult;
import com.amazonaws.services.ec2.model.DescribeConversionTasksRequest;
import com.amazonaws.services.ec2.model.DescribeConversionTasksResult;
import com.amazonaws.services.ec2.model.DescribeCustomerGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeCustomerGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeDhcpOptionsRequest;
import com.amazonaws.services.ec2.model.DescribeDhcpOptionsResult;
import com.amazonaws.services.ec2.model.DescribeEgressOnlyInternetGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeEgressOnlyInternetGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeElasticGpusRequest;
import com.amazonaws.services.ec2.model.DescribeElasticGpusResult;
import com.amazonaws.services.ec2.model.DescribeExportTasksRequest;
import com.amazonaws.services.ec2.model.DescribeExportTasksResult;
import com.amazonaws.services.ec2.model.DescribeFleetHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeFleetHistoryResult;
import com.amazonaws.services.ec2.model.DescribeFleetInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeFleetInstancesResult;
import com.amazonaws.services.ec2.model.DescribeFleetsRequest;
import com.amazonaws.services.ec2.model.DescribeFleetsResult;
import com.amazonaws.services.ec2.model.DescribeFlowLogsRequest;
import com.amazonaws.services.ec2.model.DescribeFlowLogsResult;
import com.amazonaws.services.ec2.model.DescribeFpgaImageAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeFpgaImageAttributeResult;
import com.amazonaws.services.ec2.model.DescribeFpgaImagesRequest;
import com.amazonaws.services.ec2.model.DescribeFpgaImagesResult;
import com.amazonaws.services.ec2.model.DescribeHostReservationOfferingsRequest;
import com.amazonaws.services.ec2.model.DescribeHostReservationOfferingsResult;
import com.amazonaws.services.ec2.model.DescribeHostReservationsRequest;
import com.amazonaws.services.ec2.model.DescribeHostReservationsResult;
import com.amazonaws.services.ec2.model.DescribeHostsRequest;
import com.amazonaws.services.ec2.model.DescribeHostsResult;
import com.amazonaws.services.ec2.model.DescribeIamInstanceProfileAssociationsRequest;
import com.amazonaws.services.ec2.model.DescribeIamInstanceProfileAssociationsResult;
import com.amazonaws.services.ec2.model.DescribeIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribeIdFormatResult;
import com.amazonaws.services.ec2.model.DescribeIdentityIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribeIdentityIdFormatResult;
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
import com.amazonaws.services.ec2.model.DescribeInstanceCreditSpecificationsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceCreditSpecificationsResult;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeInternetGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeInternetGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeLaunchTemplateVersionsRequest;
import com.amazonaws.services.ec2.model.DescribeLaunchTemplateVersionsResult;
import com.amazonaws.services.ec2.model.DescribeLaunchTemplatesRequest;
import com.amazonaws.services.ec2.model.DescribeLaunchTemplatesResult;
import com.amazonaws.services.ec2.model.DescribeMovingAddressesRequest;
import com.amazonaws.services.ec2.model.DescribeMovingAddressesResult;
import com.amazonaws.services.ec2.model.DescribeNatGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeNatGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkAclsResult;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfaceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfaceAttributeResult;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacePermissionsRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacePermissionsResult;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacesRequest;
import com.amazonaws.services.ec2.model.DescribeNetworkInterfacesResult;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsRequest;
import com.amazonaws.services.ec2.model.DescribePlacementGroupsResult;
import com.amazonaws.services.ec2.model.DescribePrefixListsRequest;
import com.amazonaws.services.ec2.model.DescribePrefixListsResult;
import com.amazonaws.services.ec2.model.DescribePrincipalIdFormatRequest;
import com.amazonaws.services.ec2.model.DescribePrincipalIdFormatResult;
import com.amazonaws.services.ec2.model.DescribePublicIpv4PoolsRequest;
import com.amazonaws.services.ec2.model.DescribePublicIpv4PoolsResult;
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
import com.amazonaws.services.ec2.model.DescribeSecurityGroupReferencesRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupReferencesResult;
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
import com.amazonaws.services.ec2.model.DescribeStaleSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeStaleSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.DescribeTagsRequest;
import com.amazonaws.services.ec2.model.DescribeTagsResult;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayAttachmentsRequest;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayAttachmentsResult;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayRouteTablesRequest;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayRouteTablesResult;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayVpcAttachmentsRequest;
import com.amazonaws.services.ec2.model.DescribeTransitGatewayVpcAttachmentsResult;
import com.amazonaws.services.ec2.model.DescribeTransitGatewaysRequest;
import com.amazonaws.services.ec2.model.DescribeTransitGatewaysResult;
import com.amazonaws.services.ec2.model.DescribeVolumeAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeVolumeAttributeResult;
import com.amazonaws.services.ec2.model.DescribeVolumeStatusRequest;
import com.amazonaws.services.ec2.model.DescribeVolumeStatusResult;
import com.amazonaws.services.ec2.model.DescribeVolumesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.DescribeVpcAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeVpcAttributeResult;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.DescribeVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointConnectionNotificationsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointConnectionNotificationsResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointConnectionsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointConnectionsResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServiceConfigurationsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServiceConfigurationsResult;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServicePermissionsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointServicePermissionsResult;
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
import com.amazonaws.services.ec2.model.DisableTransitGatewayRouteTablePropagationRequest;
import com.amazonaws.services.ec2.model.DisableTransitGatewayRouteTablePropagationResult;
import com.amazonaws.services.ec2.model.DisableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.DisableVgwRoutePropagationResult;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.DisableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.DisassociateAddressResult;
import com.amazonaws.services.ec2.model.DisassociateClientVpnTargetNetworkRequest;
import com.amazonaws.services.ec2.model.DisassociateClientVpnTargetNetworkResult;
import com.amazonaws.services.ec2.model.DisassociateIamInstanceProfileRequest;
import com.amazonaws.services.ec2.model.DisassociateIamInstanceProfileResult;
import com.amazonaws.services.ec2.model.DisassociateRouteTableRequest;
import com.amazonaws.services.ec2.model.DisassociateRouteTableResult;
import com.amazonaws.services.ec2.model.DisassociateSubnetCidrBlockRequest;
import com.amazonaws.services.ec2.model.DisassociateSubnetCidrBlockResult;
import com.amazonaws.services.ec2.model.DisassociateTransitGatewayRouteTableRequest;
import com.amazonaws.services.ec2.model.DisassociateTransitGatewayRouteTableResult;
import com.amazonaws.services.ec2.model.DisassociateVpcCidrBlockRequest;
import com.amazonaws.services.ec2.model.DisassociateVpcCidrBlockResult;
import com.amazonaws.services.ec2.model.DryRunResult;
import com.amazonaws.services.ec2.model.DryRunSupportedRequest;
import com.amazonaws.services.ec2.model.EnableTransitGatewayRouteTablePropagationRequest;
import com.amazonaws.services.ec2.model.EnableTransitGatewayRouteTablePropagationResult;
import com.amazonaws.services.ec2.model.EnableVgwRoutePropagationRequest;
import com.amazonaws.services.ec2.model.EnableVgwRoutePropagationResult;
import com.amazonaws.services.ec2.model.EnableVolumeIORequest;
import com.amazonaws.services.ec2.model.EnableVolumeIOResult;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkDnsSupportResult;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkRequest;
import com.amazonaws.services.ec2.model.EnableVpcClassicLinkResult;
import com.amazonaws.services.ec2.model.ExportClientVpnClientCertificateRevocationListRequest;
import com.amazonaws.services.ec2.model.ExportClientVpnClientCertificateRevocationListResult;
import com.amazonaws.services.ec2.model.ExportClientVpnClientConfigurationRequest;
import com.amazonaws.services.ec2.model.ExportClientVpnClientConfigurationResult;
import com.amazonaws.services.ec2.model.ExportTransitGatewayRoutesRequest;
import com.amazonaws.services.ec2.model.ExportTransitGatewayRoutesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GetConsoleOutputRequest;
import com.amazonaws.services.ec2.model.GetConsoleOutputResult;
import com.amazonaws.services.ec2.model.GetConsoleScreenshotRequest;
import com.amazonaws.services.ec2.model.GetConsoleScreenshotResult;
import com.amazonaws.services.ec2.model.GetHostReservationPurchasePreviewRequest;
import com.amazonaws.services.ec2.model.GetHostReservationPurchasePreviewResult;
import com.amazonaws.services.ec2.model.GetLaunchTemplateDataRequest;
import com.amazonaws.services.ec2.model.GetLaunchTemplateDataResult;
import com.amazonaws.services.ec2.model.GetPasswordDataRequest;
import com.amazonaws.services.ec2.model.GetPasswordDataResult;
import com.amazonaws.services.ec2.model.GetReservedInstancesExchangeQuoteRequest;
import com.amazonaws.services.ec2.model.GetReservedInstancesExchangeQuoteResult;
import com.amazonaws.services.ec2.model.GetTransitGatewayAttachmentPropagationsRequest;
import com.amazonaws.services.ec2.model.GetTransitGatewayAttachmentPropagationsResult;
import com.amazonaws.services.ec2.model.GetTransitGatewayRouteTableAssociationsRequest;
import com.amazonaws.services.ec2.model.GetTransitGatewayRouteTableAssociationsResult;
import com.amazonaws.services.ec2.model.GetTransitGatewayRouteTablePropagationsRequest;
import com.amazonaws.services.ec2.model.GetTransitGatewayRouteTablePropagationsResult;
import com.amazonaws.services.ec2.model.ImportClientVpnClientCertificateRevocationListRequest;
import com.amazonaws.services.ec2.model.ImportClientVpnClientCertificateRevocationListResult;
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
import com.amazonaws.services.ec2.model.ModifyCapacityReservationRequest;
import com.amazonaws.services.ec2.model.ModifyCapacityReservationResult;
import com.amazonaws.services.ec2.model.ModifyClientVpnEndpointRequest;
import com.amazonaws.services.ec2.model.ModifyClientVpnEndpointResult;
import com.amazonaws.services.ec2.model.ModifyFleetRequest;
import com.amazonaws.services.ec2.model.ModifyFleetResult;
import com.amazonaws.services.ec2.model.ModifyFpgaImageAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyFpgaImageAttributeResult;
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
import com.amazonaws.services.ec2.model.ModifyInstanceCapacityReservationAttributesRequest;
import com.amazonaws.services.ec2.model.ModifyInstanceCapacityReservationAttributesResult;
import com.amazonaws.services.ec2.model.ModifyInstanceCreditSpecificationRequest;
import com.amazonaws.services.ec2.model.ModifyInstanceCreditSpecificationResult;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementRequest;
import com.amazonaws.services.ec2.model.ModifyInstancePlacementResult;
import com.amazonaws.services.ec2.model.ModifyLaunchTemplateRequest;
import com.amazonaws.services.ec2.model.ModifyLaunchTemplateResult;
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
import com.amazonaws.services.ec2.model.ModifyTransitGatewayVpcAttachmentRequest;
import com.amazonaws.services.ec2.model.ModifyTransitGatewayVpcAttachmentResult;
import com.amazonaws.services.ec2.model.ModifyVolumeAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVolumeAttributeResult;
import com.amazonaws.services.ec2.model.ModifyVolumeRequest;
import com.amazonaws.services.ec2.model.ModifyVolumeResult;
import com.amazonaws.services.ec2.model.ModifyVpcAttributeRequest;
import com.amazonaws.services.ec2.model.ModifyVpcAttributeResult;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointConnectionNotificationRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointConnectionNotificationResult;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointResult;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointServiceConfigurationRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointServiceConfigurationResult;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointServicePermissionsRequest;
import com.amazonaws.services.ec2.model.ModifyVpcEndpointServicePermissionsResult;
import com.amazonaws.services.ec2.model.ModifyVpcPeeringConnectionOptionsRequest;
import com.amazonaws.services.ec2.model.ModifyVpcPeeringConnectionOptionsResult;
import com.amazonaws.services.ec2.model.ModifyVpcTenancyRequest;
import com.amazonaws.services.ec2.model.ModifyVpcTenancyResult;
import com.amazonaws.services.ec2.model.MonitorInstancesRequest;
import com.amazonaws.services.ec2.model.MonitorInstancesResult;
import com.amazonaws.services.ec2.model.MoveAddressToVpcRequest;
import com.amazonaws.services.ec2.model.MoveAddressToVpcResult;
import com.amazonaws.services.ec2.model.ProvisionByoipCidrRequest;
import com.amazonaws.services.ec2.model.ProvisionByoipCidrResult;
import com.amazonaws.services.ec2.model.PurchaseHostReservationRequest;
import com.amazonaws.services.ec2.model.PurchaseHostReservationResult;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingRequest;
import com.amazonaws.services.ec2.model.PurchaseReservedInstancesOfferingResult;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.PurchaseScheduledInstancesResult;
import com.amazonaws.services.ec2.model.RebootInstancesRequest;
import com.amazonaws.services.ec2.model.RebootInstancesResult;
import com.amazonaws.services.ec2.model.RegisterImageRequest;
import com.amazonaws.services.ec2.model.RegisterImageResult;
import com.amazonaws.services.ec2.model.RejectTransitGatewayVpcAttachmentRequest;
import com.amazonaws.services.ec2.model.RejectTransitGatewayVpcAttachmentResult;
import com.amazonaws.services.ec2.model.RejectVpcEndpointConnectionsRequest;
import com.amazonaws.services.ec2.model.RejectVpcEndpointConnectionsResult;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionRequest;
import com.amazonaws.services.ec2.model.RejectVpcPeeringConnectionResult;
import com.amazonaws.services.ec2.model.ReleaseAddressRequest;
import com.amazonaws.services.ec2.model.ReleaseAddressResult;
import com.amazonaws.services.ec2.model.ReleaseHostsRequest;
import com.amazonaws.services.ec2.model.ReleaseHostsResult;
import com.amazonaws.services.ec2.model.ReplaceIamInstanceProfileAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceIamInstanceProfileAssociationResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclAssociationResult;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclEntryRequest;
import com.amazonaws.services.ec2.model.ReplaceNetworkAclEntryResult;
import com.amazonaws.services.ec2.model.ReplaceRouteRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteResult;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationRequest;
import com.amazonaws.services.ec2.model.ReplaceRouteTableAssociationResult;
import com.amazonaws.services.ec2.model.ReplaceTransitGatewayRouteRequest;
import com.amazonaws.services.ec2.model.ReplaceTransitGatewayRouteResult;
import com.amazonaws.services.ec2.model.ReportInstanceStatusRequest;
import com.amazonaws.services.ec2.model.ReportInstanceStatusResult;
import com.amazonaws.services.ec2.model.RequestSpotFleetRequest;
import com.amazonaws.services.ec2.model.RequestSpotFleetResult;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResetFpgaImageAttributeRequest;
import com.amazonaws.services.ec2.model.ResetFpgaImageAttributeResult;
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
import com.amazonaws.services.ec2.model.RevokeClientVpnIngressRequest;
import com.amazonaws.services.ec2.model.RevokeClientVpnIngressResult;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupEgressResult;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.RunScheduledInstancesRequest;
import com.amazonaws.services.ec2.model.RunScheduledInstancesResult;
import com.amazonaws.services.ec2.model.SearchTransitGatewayRoutesRequest;
import com.amazonaws.services.ec2.model.SearchTransitGatewayRoutesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateClientVpnConnectionsRequest;
import com.amazonaws.services.ec2.model.TerminateClientVpnConnectionsResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.model.UnassignIpv6AddressesRequest;
import com.amazonaws.services.ec2.model.UnassignIpv6AddressesResult;
import com.amazonaws.services.ec2.model.UnassignPrivateIpAddressesRequest;
import com.amazonaws.services.ec2.model.UnassignPrivateIpAddressesResult;
import com.amazonaws.services.ec2.model.UnmonitorInstancesRequest;
import com.amazonaws.services.ec2.model.UnmonitorInstancesResult;
import com.amazonaws.services.ec2.model.UpdateSecurityGroupRuleDescriptionsEgressRequest;
import com.amazonaws.services.ec2.model.UpdateSecurityGroupRuleDescriptionsEgressResult;
import com.amazonaws.services.ec2.model.UpdateSecurityGroupRuleDescriptionsIngressRequest;
import com.amazonaws.services.ec2.model.UpdateSecurityGroupRuleDescriptionsIngressResult;
import com.amazonaws.services.ec2.model.WithdrawByoipCidrRequest;
import com.amazonaws.services.ec2.model.WithdrawByoipCidrResult;
import com.amazonaws.services.ec2.waiters.AmazonEC2Waiters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonEC2Mock implements AmazonEC2 {

    private static final Logger logger = LogManager.getLogger(AmazonEC2Mock.class);

    public static final String PREFIX_PRIVATE_IP = "10.0.0.";
    public static final String PREFIX_PUBLIC_IP = "8.8.8.";
    public static final String PREFIX_PUBLIC_DNS = "mock-ec2-";
    public static final String SUFFIX_PUBLIC_DNS = ".amazon.com";
    public static final String PREFIX_PRIVATE_DNS = "mock-ip-";
    public static final String SUFFIX_PRIVATE_DNS = ".ec2.internal";

    final List<Instance> instances = new ArrayList<>();
    String endpoint;
    final AWSCredentialsProvider credentials;
    final ClientConfiguration configuration;

    public AmazonEC2Mock(int nodes, List<List<Tag>> tagsList, AWSCredentialsProvider credentials, ClientConfiguration configuration) {
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
        this.credentials = credentials;
        this.configuration = configuration;
    }

    @Override
    public DescribeInstancesResult describeInstances(DescribeInstancesRequest describeInstancesRequest)
            throws AmazonClientException {
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
        this.endpoint = endpoint;
    }

    @Override
    public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptReservedInstancesExchangeQuoteResult acceptReservedInstancesExchangeQuote(
            AcceptReservedInstancesExchangeQuoteRequest acceptReservedInstancesExchangeQuoteRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptTransitGatewayVpcAttachmentResult acceptTransitGatewayVpcAttachment(
        AcceptTransitGatewayVpcAttachmentRequest acceptTransitGatewayVpcAttachmentRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcEndpointConnectionsResult acceptVpcEndpointConnections(
        AcceptVpcEndpointConnectionsRequest acceptVpcEndpointConnectionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RebootInstancesResult rebootInstances(RebootInstancesRequest rebootInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances(
            DescribeReservedInstancesRequest describeReservedInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateFlowLogsResult createFlowLogs(CreateFlowLogsRequest createFlowLogsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones(DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RestoreAddressToClassicResult restoreAddressToClassic(RestoreAddressToClassicRequest restoreAddressToClassicRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeClientVpnIngressResult revokeClientVpnIngress(RevokeClientVpnIngressRequest revokeClientVpnIngressRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachVolumeResult detachVolume(DetachVolumeRequest detachVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteKeyPairResult deleteKeyPair(DeleteKeyPairRequest deleteKeyPairRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteLaunchTemplateResult deleteLaunchTemplate(DeleteLaunchTemplateRequest deleteLaunchTemplateRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteLaunchTemplateVersionsResult deleteLaunchTemplateVersions(
        DeleteLaunchTemplateVersionsRequest deleteLaunchTemplateVersionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNatGatewayResult deleteNatGateway(DeleteNatGatewayRequest deleteNatGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnmonitorInstancesResult unmonitorInstances(UnmonitorInstancesRequest unmonitorInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UpdateSecurityGroupRuleDescriptionsIngressResult updateSecurityGroupRuleDescriptionsIngress(
            UpdateSecurityGroupRuleDescriptionsIngressRequest updateSecurityGroupRuleDescriptionsIngressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public WithdrawByoipCidrResult withdrawByoipCidr(WithdrawByoipCidrRequest withdrawByoipCidrRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UpdateSecurityGroupRuleDescriptionsEgressResult updateSecurityGroupRuleDescriptionsEgress(
            UpdateSecurityGroupRuleDescriptionsEgressRequest updateSecurityGroupRuleDescriptionsEgressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVpnGatewayResult attachVpnGateway(AttachVpnGatewayRequest attachVpnGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AuthorizeClientVpnIngressResult authorizeClientVpnIngress(AuthorizeClientVpnIngressRequest authorizeClientVpnIngressRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateImageResult createImage(CreateImageRequest createImageRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSecurityGroupResult deleteSecurityGroup(DeleteSecurityGroupRequest deleteSecurityGroupRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInstanceExportTaskResult createInstanceExportTask(CreateInstanceExportTaskRequest createInstanceExportTaskRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AuthorizeSecurityGroupEgressResult authorizeSecurityGroupEgress(
            AuthorizeSecurityGroupEgressRequest authorizeSecurityGroupEgressRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateDhcpOptionsResult associateDhcpOptions(AssociateDhcpOptionsRequest associateDhcpOptionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetPasswordDataResult getPasswordData(GetPasswordDataRequest getPasswordDataRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetReservedInstancesExchangeQuoteResult getReservedInstancesExchangeQuote(
            GetReservedInstancesExchangeQuoteRequest getReservedInstancesExchangeQuoteRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetTransitGatewayAttachmentPropagationsResult getTransitGatewayAttachmentPropagations(
        GetTransitGatewayAttachmentPropagationsRequest getTransitGatewayAttachmentPropagationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetTransitGatewayRouteTableAssociationsResult getTransitGatewayRouteTableAssociations(
        GetTransitGatewayRouteTableAssociationsRequest getTransitGatewayRouteTableAssociationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetTransitGatewayRouteTablePropagationsResult getTransitGatewayRouteTablePropagations(
        GetTransitGatewayRouteTablePropagationsRequest getTransitGatewayRouteTablePropagationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportClientVpnClientCertificateRevocationListResult importClientVpnClientCertificateRevocationList(
        ImportClientVpnClientCertificateRevocationListRequest importClientVpnClientCertificateRevocationListRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StopInstancesResult stopInstances(StopInstancesRequest stopInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public TerminateClientVpnConnectionsResult terminateClientVpnConnections(
        TerminateClientVpnConnectionsRequest terminateClientVpnConnectionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportKeyPairResult importKeyPair(ImportKeyPairRequest importKeyPairRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkInterfaceResult deleteNetworkInterface(DeleteNetworkInterfaceRequest deleteNetworkInterfaceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcAttributeResult modifyVpcAttribute(ModifyVpcAttributeRequest modifyVpcAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetInstancesResult describeSpotFleetInstances(DescribeSpotFleetInstancesRequest describeSpotFleetInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSecurityGroupResult createSecurityGroup(CreateSecurityGroupRequest createSecurityGroupRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces(DescribeNetworkInterfacesRequest describeNetworkInterfacesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacePermissionsResult describeNetworkInterfacePermissions(
            DescribeNetworkInterfacePermissionsRequest describeNetworkInterfacePermissionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRegionsResult describeRegions(DescribeRegionsRequest describeRegionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateDhcpOptionsResult createDhcpOptions(CreateDhcpOptionsRequest createDhcpOptionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateReservedInstancesListingResult createReservedInstancesListing(
            CreateReservedInstancesListingRequest createReservedInstancesListingRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcEndpointsResult deleteVpcEndpoints(DeleteVpcEndpointsRequest deleteVpcEndpointsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetSnapshotAttributeResult resetSnapshotAttribute(ResetSnapshotAttributeRequest resetSnapshotAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteRouteResult deleteRoute(DeleteRouteRequest deleteRouteRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways(DescribeInternetGatewaysRequest describeInternetGatewaysRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportVolumeResult importVolume(ImportVolumeRequest importVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyCapacityReservationResult modifyCapacityReservation(ModifyCapacityReservationRequest modifyCapacityReservationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyClientVpnEndpointResult modifyClientVpnEndpoint(ModifyClientVpnEndpointRequest modifyClientVpnEndpointRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyFleetResult modifyFleet(ModifyFleetRequest modifyFleetRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyFpgaImageAttributeResult modifyFpgaImageAttribute(ModifyFpgaImageAttributeRequest modifyFpgaImageAttributeRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyHostsResult modifyHosts(ModifyHostsRequest modifyHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyIdFormatResult modifyIdFormat(ModifyIdFormatRequest modifyIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups(DescribeSecurityGroupsRequest describeSecurityGroupsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeStaleSecurityGroupsResult describeStaleSecurityGroups(
            DescribeStaleSecurityGroupsRequest describeStaleSecurityGroupsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupReferencesResult describeSecurityGroupReferences(
            DescribeSecurityGroupReferencesRequest describeSecurityGroupReferencesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RejectVpcPeeringConnectionResult rejectVpcPeeringConnection(
            RejectVpcPeeringConnectionRequest rejectVpcPeeringConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcPeeringConnectionOptionsResult modifyVpcPeeringConnectionOptions(
            ModifyVpcPeeringConnectionOptionsRequest modifyVpcPeeringConnectionOptionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcTenancyResult modifyVpcTenancy(ModifyVpcTenancyRequest modifyVpcTenancyRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteFlowLogsResult deleteFlowLogs(DeleteFlowLogsRequest deleteFlowLogsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteFpgaImageResult deleteFpgaImage(DeleteFpgaImageRequest deleteFpgaImageRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachVpnGatewayResult detachVpnGateway(DetachVpnGatewayRequest detachVpnGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableTransitGatewayRouteTablePropagationResult disableTransitGatewayRouteTablePropagation(
        DisableTransitGatewayRouteTablePropagationRequest disableTransitGatewayRouteTablePropagationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeregisterImageResult deregisterImage(DeregisterImageRequest deregisterImageRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription(
                DescribeSpotDatafeedSubscriptionRequest describeSpotDatafeedSubscriptionRequest)
                throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTagsResult deleteTags(DeleteTagsRequest deleteTagsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTransitGatewayResult deleteTransitGateway(DeleteTransitGatewayRequest deleteTransitGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTransitGatewayRouteResult deleteTransitGatewayRoute(DeleteTransitGatewayRouteRequest deleteTransitGatewayRouteRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTransitGatewayRouteTableResult deleteTransitGatewayRouteTable(
        DeleteTransitGatewayRouteTableRequest deleteTransitGatewayRouteTableRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteTransitGatewayVpcAttachmentResult deleteTransitGatewayVpcAttachment(
        DeleteTransitGatewayVpcAttachmentRequest deleteTransitGatewayVpcAttachmentRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSubnetResult deleteSubnet(DeleteSubnetRequest deleteSubnetRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes(DescribeAccountAttributesRequest describeAccountAttributesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachClassicLinkVpcResult attachClassicLinkVpc(AttachClassicLinkVpcRequest attachClassicLinkVpcRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnGatewayResult createVpnGateway(CreateVpnGatewayRequest createVpnGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteClientVpnEndpointResult deleteClientVpnEndpoint(DeleteClientVpnEndpointRequest deleteClientVpnEndpointRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteClientVpnRouteResult deleteClientVpnRoute(DeleteClientVpnRouteRequest deleteClientVpnRouteRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVolumeIOResult enableVolumeIO(EnableVolumeIORequest enableVolumeIORequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MoveAddressToVpcResult moveAddressToVpc(MoveAddressToVpcRequest moveAddressToVpcRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ProvisionByoipCidrResult provisionByoipCidr(ProvisionByoipCidrRequest provisionByoipCidrRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnGatewayResult deleteVpnGateway(DeleteVpnGatewayRequest deleteVpnGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeprovisionByoipCidrResult deprovisionByoipCidr(DeprovisionByoipCidrRequest deprovisionByoipCidrRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachVolumeResult attachVolume(AttachVolumeRequest attachVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus(DescribeVolumeStatusRequest describeVolumeStatusRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesModificationsResult describeVolumesModifications(
            DescribeVolumesModificationsRequest describeVolumesModificationsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks(
            DescribeImportSnapshotTasksRequest describeImportSnapshotTasksRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections(DescribeVpnConnectionsRequest describeVpnConnectionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetImageAttributeResult resetImageAttribute(ResetImageAttributeRequest resetImageAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVgwRoutePropagationResult enableVgwRoutePropagation(EnableVgwRoutePropagationRequest enableVgwRoutePropagationRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSnapshotResult createSnapshot(CreateSnapshotRequest createSnapshotRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVolumeResult deleteVolume(DeleteVolumeRequest deleteVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkInterfaceResult createNetworkInterface(CreateNetworkInterfaceRequest createNetworkInterfaceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyReservedInstancesResult modifyReservedInstances(ModifyReservedInstancesRequest modifyReservedInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotFleetRequestsResult cancelSpotFleetRequests(CancelSpotFleetRequestsRequest cancelSpotFleetRequestsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnassignPrivateIpAddressesResult unassignPrivateIpAddresses(UnassignPrivateIpAddressesRequest unassignPrivateIpAddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public UnassignIpv6AddressesResult unassignIpv6Addresses(UnassignIpv6AddressesRequest unassignIpv6AddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcsResult describeVpcs(DescribeVpcsRequest describeVpcsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelConversionTaskResult cancelConversionTask(CancelConversionTaskRequest cancelConversionTaskRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateAddressResult associateAddress(AssociateAddressRequest associateAddressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateClientVpnTargetNetworkResult associateClientVpnTargetNetwork(
        AssociateClientVpnTargetNetworkRequest associateClientVpnTargetNetworkRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateIamInstanceProfileResult associateIamInstanceProfile(AssociateIamInstanceProfileRequest associateIamInstanceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateVpcCidrBlockResult associateVpcCidrBlock(AssociateVpcCidrBlockRequest associateVpcCidrBlockRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateSubnetCidrBlockResult associateSubnetCidrBlock(AssociateSubnetCidrBlockRequest associateSubnetCidrBlockRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateTransitGatewayRouteTableResult associateTransitGatewayRouteTable(
        AssociateTransitGatewayRouteTableRequest associateTransitGatewayRouteTableRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteCustomerGatewayResult deleteCustomerGateway(DeleteCustomerGatewayRequest deleteCustomerGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkAclEntryResult createNetworkAclEntry(CreateNetworkAclEntryRequest createNetworkAclEntryRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection(AcceptVpcPeeringConnectionRequest acceptVpcPeeringConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeExportTasksResult describeExportTasks(DescribeExportTasksRequest describeExportTasksRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeElasticGpusResult describeElasticGpus(DescribeElasticGpusRequest describeElasticGpusRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFpgaImagesResult describeFpgaImages(DescribeFpgaImagesRequest describeFpgaImagesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeHostReservationOfferingsResult describeHostReservationOfferings(
            DescribeHostReservationOfferingsRequest describeHostReservationOfferingsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeHostReservationsResult describeHostReservations(DescribeHostReservationsRequest describeHostReservationsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeIdentityIdFormatResult describeIdentityIdFormat(DescribeIdentityIdFormatRequest describeIdentityIdFormatRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachInternetGatewayResult detachInternetGateway(DetachInternetGatewayRequest detachInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection(CreateVpcPeeringConnectionRequest createVpcPeeringConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteTableResult createRouteTable(CreateRouteTableRequest createRouteTableRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask(CancelImportTaskRequest cancelImportTaskRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesResult describeVolumes(DescribeVolumesRequest describeVolumesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings(
            DescribeReservedInstancesListingsRequest describeReservedInstancesListingsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReportInstanceStatusResult reportInstanceStatus(ReportInstanceStatusRequest reportInstanceStatusRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables(DescribeRouteTablesRequest describeRouteTablesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions(DescribeDhcpOptionsRequest describeDhcpOptionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }
    
    @Override
    public DescribeEgressOnlyInternetGatewaysResult describeEgressOnlyInternetGateways(
            DescribeEgressOnlyInternetGatewaysRequest describeEgressOnlyInternetGatewaysRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public MonitorInstancesResult monitorInstances(MonitorInstancesRequest monitorInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrefixListsResult describePrefixLists(DescribePrefixListsRequest describePrefixListsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotFleetResult requestSpotFleet(RequestSpotFleetRequest requestSpotFleetRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks(DescribeImportImageTasksRequest describeImportImageTasksRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls(DescribeNetworkAclsRequest describeNetworkAclsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks(DescribeBundleTasksRequest describeBundleTasksRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportInstanceResult importInstance(ImportInstanceRequest importInstanceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcPeeringConnectionResult deleteVpcPeeringConnection(DeleteVpcPeeringConnectionRequest deleteVpcPeeringConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetConsoleOutputResult getConsoleOutput(GetConsoleOutputRequest getConsoleOutputRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetConsoleScreenshotResult getConsoleScreenshot(GetConsoleScreenshotRequest getConsoleScreenshotRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetHostReservationPurchasePreviewResult getHostReservationPurchasePreview(
            GetHostReservationPurchasePreviewRequest getHostReservationPurchasePreviewRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public GetLaunchTemplateDataResult getLaunchTemplateData(GetLaunchTemplateDataRequest getLaunchTemplateDataRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway(CreateInternetGatewayRequest createInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnConnectionRouteResult deleteVpnConnectionRoute(DeleteVpnConnectionRouteRequest deleteVpnConnectionRouteRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachNetworkInterfaceResult detachNetworkInterface(DetachNetworkInterfaceRequest detachNetworkInterfaceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyImageAttributeResult modifyImageAttribute(ModifyImageAttributeRequest modifyImageAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateCustomerGatewayResult createCustomerGateway(CreateCustomerGatewayRequest createCustomerGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateDefaultSubnetResult createDefaultSubnet(CreateDefaultSubnetRequest createDefaultSubnetRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateEgressOnlyInternetGatewayResult createEgressOnlyInternetGateway(
            CreateEgressOnlyInternetGatewayRequest createEgressOnlyInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateFleetResult createFleet(CreateFleetRequest createFleetRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateFpgaImageResult createFpgaImage(CreateFpgaImageRequest createFpgaImageRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkInterfacePermissionResult createNetworkInterfacePermission(
            CreateNetworkInterfacePermissionRequest createNetworkInterfacePermissionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateDefaultVpcResult createDefaultVpc(CreateDefaultVpcRequest createDefaultVpcRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSpotDatafeedSubscriptionResult createSpotDatafeedSubscription(
            CreateSpotDatafeedSubscriptionRequest createSpotDatafeedSubscriptionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachInternetGatewayResult attachInternetGateway(AttachInternetGatewayRequest attachInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpnConnectionResult deleteVpnConnection(DeleteVpnConnectionRequest deleteVpnConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses(DescribeMovingAddressesRequest describeMovingAddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks(DescribeConversionTasksRequest describeConversionTasksRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnConnectionResult createVpnConnection(CreateVpnConnectionRequest createVpnConnectionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage(ImportImageRequest importImageRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkResult disableVpcClassicLink(DisableVpcClassicLinkRequest disableVpcClassicLinkRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVpcClassicLinkDnsSupportResult disableVpcClassicLinkDnsSupport(
            DisableVpcClassicLinkDnsSupportRequest disableVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceAttributeResult describeInstanceAttribute(DescribeInstanceAttributeRequest describeInstanceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceCreditSpecificationsResult describeInstanceCreditSpecifications(
        DescribeInstanceCreditSpecificationsRequest describeInstanceCreditSpecificationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs(DescribeFlowLogsRequest describeFlowLogsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections(
            DescribeVpcPeeringConnectionsRequest describeVpcPeeringConnectionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups(DescribePlacementGroupsRequest describePlacementGroupsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RunInstancesResult runInstances(RunInstancesRequest runInstancesRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RunScheduledInstancesResult runScheduledInstances(RunScheduledInstancesRequest runScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public SearchTransitGatewayRoutesResult searchTransitGatewayRoutes(
        SearchTransitGatewayRoutesRequest searchTransitGatewayRoutesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets(DescribeSubnetsRequest describeSubnetsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssociateRouteTableResult associateRouteTable(AssociateRouteTableRequest associateRouteTableRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVolumeAttributeResult modifyVolumeAttribute(ModifyVolumeAttributeRequest modifyVolumeAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkAclResult deleteNetworkAcl(DeleteNetworkAclRequest deleteNetworkAclRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages(DescribeImagesRequest describeImagesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public StartInstancesResult startInstances(StartInstancesRequest startInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstanceAttributeResult modifyInstanceAttribute(ModifyInstanceAttributeRequest modifyInstanceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstanceCapacityReservationAttributesResult modifyInstanceCapacityReservationAttributes(
        ModifyInstanceCapacityReservationAttributesRequest modifyInstanceCapacityReservationAttributesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstanceCreditSpecificationResult modifyInstanceCreditSpecification(
        ModifyInstanceCreditSpecificationRequest modifyInstanceCreditSpecificationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyInstancePlacementResult modifyInstancePlacement(ModifyInstancePlacementRequest modifyInstancePlacementRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyLaunchTemplateResult modifyLaunchTemplate(ModifyLaunchTemplateRequest modifyLaunchTemplateRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyIdentityIdFormatResult modifyIdentityIdFormat(ModifyIdentityIdFormatRequest modifyIdentityIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelReservedInstancesListingResult cancelReservedInstancesListing(
            CancelReservedInstancesListingRequest cancelReservedInstancesListingRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteDhcpOptionsResult deleteDhcpOptions(DeleteDhcpOptionsRequest deleteDhcpOptionsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteEgressOnlyInternetGatewayResult deleteEgressOnlyInternetGateway(
            DeleteEgressOnlyInternetGatewayRequest deleteEgressOnlyInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteFleetsResult deleteFleets(DeleteFleetsRequest deleteFleetsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkInterfacePermissionResult deleteNetworkInterfacePermission(
            DeleteNetworkInterfacePermissionRequest deleteNetworkInterfacePermissionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AuthorizeSecurityGroupIngressResult authorizeSecurityGroupIngress(
            AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests(
            DescribeSpotInstanceRequestsRequest describeSpotInstanceRequestsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcResult createVpc(CreateVpcRequest createVpcRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways(DescribeCustomerGatewaysRequest describeCustomerGatewaysRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelExportTaskResult cancelExportTask(CancelExportTaskRequest cancelExportTaskRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateRouteResult createRoute(CreateRouteRequest createRouteRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcEndpointResult createVpcEndpoint(CreateVpcEndpointRequest createVpcEndpointRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcEndpointConnectionNotificationResult createVpcEndpointConnectionNotification(
        CreateVpcEndpointConnectionNotificationRequest createVpcEndpointConnectionNotificationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcEndpointServiceConfigurationResult createVpcEndpointServiceConfiguration(
        CreateVpcEndpointServiceConfigurationRequest createVpcEndpointServiceConfigurationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopyImageResult copyImage(CopyImageRequest copyImageRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink(DescribeVpcClassicLinkRequest describeVpcClassicLinkRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyNetworkInterfaceAttributeResult modifyNetworkInterfaceAttribute(
            ModifyNetworkInterfaceAttributeRequest modifyNetworkInterfaceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteRouteTableResult deleteRouteTable(DeleteRouteTableRequest deleteRouteTableRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfaceAttributeResult describeNetworkInterfaceAttribute(
            DescribeNetworkInterfaceAttributeRequest describeNetworkInterfaceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances(
            DescribeClassicLinkInstancesRequest describeClassicLinkInstancesRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RequestSpotInstancesResult requestSpotInstances(RequestSpotInstancesRequest requestSpotInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetFpgaImageAttributeResult resetFpgaImageAttribute(ResetFpgaImageAttributeRequest resetFpgaImageAttributeRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTagsResult createTags(CreateTagsRequest createTagsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTransitGatewayResult createTransitGateway(CreateTransitGatewayRequest createTransitGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTransitGatewayRouteResult createTransitGatewayRoute(CreateTransitGatewayRouteRequest createTransitGatewayRouteRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTransitGatewayRouteTableResult createTransitGatewayRouteTable(
        CreateTransitGatewayRouteTableRequest createTransitGatewayRouteTableRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateTransitGatewayVpcAttachmentResult createTransitGatewayVpcAttachment(
        CreateTransitGatewayVpcAttachmentRequest createTransitGatewayVpcAttachmentRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeAttributeResult describeVolumeAttribute(DescribeVolumeAttributeRequest describeVolumeAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AttachNetworkInterfaceResult attachNetworkInterface(AttachNetworkInterfaceRequest attachNetworkInterfaceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceRouteResult replaceRoute(ReplaceRouteRequest replaceRouteRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTagsResult describeTags(DescribeTagsRequest describeTagsRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelBundleTaskResult cancelBundleTask(CancelBundleTaskRequest cancelBundleTaskRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelCapacityReservationResult cancelCapacityReservation(CancelCapacityReservationRequest cancelCapacityReservationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisableVgwRoutePropagationResult disableVgwRoutePropagation(DisableVgwRoutePropagationRequest disableVgwRoutePropagationRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot(ImportSnapshotRequest importSnapshotRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelSpotInstanceRequestsResult cancelSpotInstanceRequests(CancelSpotInstanceRequestsRequest cancelSpotInstanceRequestsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests(DescribeSpotFleetRequestsRequest describeSpotFleetRequestsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseReservedInstancesOfferingResult purchaseReservedInstancesOffering(
            PurchaseReservedInstancesOfferingRequest purchaseReservedInstancesOfferingRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseScheduledInstancesResult purchaseScheduledInstances(
            PurchaseScheduledInstancesRequest purchaseScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public PurchaseHostReservationResult purchaseHostReservation(PurchaseHostReservationRequest purchaseHostReservationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySnapshotAttributeResult modifySnapshotAttribute(ModifySnapshotAttributeRequest modifySnapshotAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications(
            DescribeReservedInstancesModificationsRequest describeReservedInstancesModificationsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public TerminateInstancesResult terminateInstances(TerminateInstancesRequest terminateInstancesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointResult modifyVpcEndpoint(ModifyVpcEndpointRequest modifyVpcEndpointRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointConnectionNotificationResult modifyVpcEndpointConnectionNotification(
        ModifyVpcEndpointConnectionNotificationRequest modifyVpcEndpointConnectionNotificationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointServiceConfigurationResult modifyVpcEndpointServiceConfiguration(
        ModifyVpcEndpointServiceConfigurationRequest modifyVpcEndpointServiceConfigurationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVpcEndpointServicePermissionsResult modifyVpcEndpointServicePermissions(
        ModifyVpcEndpointServicePermissionsRequest modifyVpcEndpointServicePermissionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSpotDatafeedSubscriptionResult deleteSpotDatafeedSubscription(
            DeleteSpotDatafeedSubscriptionRequest deleteSpotDatafeedSubscriptionRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteInternetGatewayResult deleteInternetGateway(DeleteInternetGatewayRequest deleteInternetGatewayRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotAttributeResult describeSnapshotAttribute(DescribeSnapshotAttributeRequest describeSnapshotAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceRouteTableAssociationResult replaceRouteTableAssociation(
            ReplaceRouteTableAssociationRequest replaceRouteTableAssociationRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceTransitGatewayRouteResult replaceTransitGatewayRoute(
        ReplaceTransitGatewayRouteRequest replaceTransitGatewayRouteRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAddressesResult describeAddresses(DescribeAddressesRequest describeAddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImageAttributeResult describeImageAttribute(DescribeImageAttributeRequest describeImageAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs(DescribeKeyPairsRequest describeKeyPairsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ConfirmProductInstanceResult confirmProductInstance(ConfirmProductInstanceRequest confirmProductInstanceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopyFpgaImageResult copyFpgaImage(CopyFpgaImageRequest copyFpgaImageRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateRouteTableResult disassociateRouteTable(DisassociateRouteTableRequest disassociateRouteTableRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateIamInstanceProfileResult disassociateIamInstanceProfile(
            DisassociateIamInstanceProfileRequest disassociateIamInstanceProfileRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateVpcCidrBlockResult disassociateVpcCidrBlock(DisassociateVpcCidrBlockRequest disassociateVpcCidrBlockRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableTransitGatewayRouteTablePropagationResult enableTransitGatewayRouteTablePropagation(
        EnableTransitGatewayRouteTablePropagationRequest enableTransitGatewayRouteTablePropagationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateSubnetCidrBlockResult disassociateSubnetCidrBlock(
            DisassociateSubnetCidrBlockRequest disassociateSubnetCidrBlockRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateTransitGatewayRouteTableResult disassociateTransitGatewayRouteTable(
        DisassociateTransitGatewayRouteTableRequest disassociateTransitGatewayRouteTableRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcAttributeResult describeVpcAttribute(DescribeVpcAttributeRequest describeVpcAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupEgressResult revokeSecurityGroupEgress(RevokeSecurityGroupEgressRequest revokeSecurityGroupEgressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteNetworkAclEntryResult deleteNetworkAclEntry(DeleteNetworkAclEntryRequest deleteNetworkAclEntryRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVolumeResult createVolume(CreateVolumeRequest createVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyVolumeResult modifyVolume(ModifyVolumeRequest modifyVolumeRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus(DescribeInstanceStatusRequest describeInstanceStatusRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways(DescribeVpnGatewaysRequest describeVpnGatewaysRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateSubnetResult createSubnet(CreateSubnetRequest createSubnetRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings(
            DescribeReservedInstancesOfferingsRequest describeReservedInstancesOfferingsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssignPrivateIpAddressesResult assignPrivateIpAddresses(AssignPrivateIpAddressesRequest assignPrivateIpAddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AssignIpv6AddressesResult assignIpv6Addresses(AssignIpv6AddressesRequest assignIpv6AddressesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestHistoryResult describeSpotFleetRequestHistory(
            DescribeSpotFleetRequestHistoryRequest describeSpotFleetRequestHistoryRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSnapshotResult deleteSnapshot(DeleteSnapshotRequest deleteSnapshotRequest) 
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceNetworkAclAssociationResult replaceNetworkAclAssociation(
            ReplaceNetworkAclAssociationRequest replaceNetworkAclAssociationRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateAddressResult disassociateAddress(DisassociateAddressRequest disassociateAddressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DisassociateClientVpnTargetNetworkResult disassociateClientVpnTargetNetwork(
        DisassociateClientVpnTargetNetworkRequest disassociateClientVpnTargetNetworkRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreatePlacementGroupResult createPlacementGroup(CreatePlacementGroupRequest createPlacementGroupRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public BundleInstanceResult bundleInstance(BundleInstanceRequest bundleInstanceRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeletePlacementGroupResult deletePlacementGroup(DeletePlacementGroupRequest deletePlacementGroupRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySubnetAttributeResult modifySubnetAttribute(ModifySubnetAttributeRequest modifySubnetAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifyTransitGatewayVpcAttachmentResult modifyTransitGatewayVpcAttachment(
        ModifyTransitGatewayVpcAttachmentRequest modifyTransitGatewayVpcAttachmentRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcResult deleteVpc(DeleteVpcRequest deleteVpcRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcEndpointConnectionNotificationsResult deleteVpcEndpointConnectionNotifications(
        DeleteVpcEndpointConnectionNotificationsRequest deleteVpcEndpointConnectionNotificationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteVpcEndpointServiceConfigurationsResult deleteVpcEndpointServiceConfigurations(
        DeleteVpcEndpointServiceConfigurationsRequest deleteVpcEndpointServiceConfigurationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CopySnapshotResult copySnapshot(CopySnapshotRequest copySnapshotRequest) throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateCapacityReservationResult createCapacityReservation(CreateCapacityReservationRequest createCapacityReservationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateClientVpnEndpointResult createClientVpnEndpoint(CreateClientVpnEndpointRequest createClientVpnEndpointRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateClientVpnRouteResult createClientVpnRoute(CreateClientVpnRouteRequest createClientVpnRouteRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices(
            DescribeVpcEndpointServicesRequest describeVpcEndpointServicesRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateAddressResult allocateAddress(AllocateAddressRequest allocateAddressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReleaseAddressResult releaseAddress(ReleaseAddressRequest releaseAddressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReleaseHostsResult releaseHosts(ReleaseHostsRequest releaseHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceIamInstanceProfileAssociationResult replaceIamInstanceProfileAssociation(
            ReplaceIamInstanceProfileAssociationRequest replaceIamInstanceProfileAssociationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetInstanceAttributeResult resetInstanceAttribute(ResetInstanceAttributeRequest resetInstanceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateKeyPairResult createKeyPair(CreateKeyPairRequest createKeyPairRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateLaunchTemplateResult createLaunchTemplate(CreateLaunchTemplateRequest createLaunchTemplateRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateLaunchTemplateVersionResult createLaunchTemplateVersion(
        CreateLaunchTemplateVersionRequest createLaunchTemplateVersionRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNatGatewayResult createNatGateway(CreateNatGatewayRequest createNatGatewayRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ReplaceNetworkAclEntryResult replaceNetworkAclEntry(ReplaceNetworkAclEntryRequest replaceNetworkAclEntryRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots(DescribeSnapshotsRequest describeSnapshotsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateNetworkAclResult createNetworkAcl(CreateNetworkAclRequest createNetworkAclRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RegisterImageResult registerImage(RegisterImageRequest registerImageRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RejectTransitGatewayVpcAttachmentResult rejectTransitGatewayVpcAttachment(
        RejectTransitGatewayVpcAttachmentRequest rejectTransitGatewayVpcAttachmentRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RejectVpcEndpointConnectionsResult rejectVpcEndpointConnections(
        RejectVpcEndpointConnectionsRequest rejectVpcEndpointConnectionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResetNetworkInterfaceAttributeResult resetNetworkInterfaceAttribute(
            ResetNetworkInterfaceAttributeRequest resetNetworkInterfaceAttributeRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkResult enableVpcClassicLink(EnableVpcClassicLinkRequest enableVpcClassicLinkRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public EnableVpcClassicLinkDnsSupportResult enableVpcClassicLinkDnsSupport(
            EnableVpcClassicLinkDnsSupportRequest enableVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ExportClientVpnClientCertificateRevocationListResult exportClientVpnClientCertificateRevocationList(
        ExportClientVpnClientCertificateRevocationListRequest exportClientVpnClientCertificateRevocationListRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ExportClientVpnClientConfigurationResult exportClientVpnClientConfiguration(
        ExportClientVpnClientConfigurationRequest exportClientVpnClientConfigurationRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ExportTransitGatewayRoutesResult exportTransitGatewayRoutes(
        ExportTransitGatewayRoutesRequest exportTransitGatewayRoutesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpnConnectionRouteResult createVpnConnectionRoute(CreateVpnConnectionRouteRequest createVpnConnectionRouteRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints(DescribeVpcEndpointsRequest describeVpcEndpointsRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DetachClassicLinkVpcResult detachClassicLinkVpc(DetachClassicLinkVpcRequest detachClassicLinkVpcRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesResult describeReservedInstances() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkInterfacesResult describeNetworkInterfaces() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRegionsResult describeRegions() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInternetGatewaysResult describeInternetGateways() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSecurityGroupsResult describeSecurityGroups() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotDatafeedSubscriptionResult describeSpotDatafeedSubscription() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAccountAttributesResult describeAccountAttributes() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumeStatusResult describeVolumeStatus() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportSnapshotTasksResult describeImportSnapshotTasks() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnConnectionsResult describeVpnConnections() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcsResult describeVpcs() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AcceptVpcPeeringConnectionResult acceptVpcPeeringConnection() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AdvertiseByoipCidrResult advertiseByoipCidr(AdvertiseByoipCidrRequest advertiseByoipCidrRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeExportTasksResult describeExportTasks() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFleetHistoryResult describeFleetHistory(DescribeFleetHistoryRequest describeFleetHistoryRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFleetInstancesResult describeFleetInstances(DescribeFleetInstancesRequest describeFleetInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFleetsResult describeFleets(DescribeFleetsRequest describeFleetsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateVpcPeeringConnectionResult createVpcPeeringConnection() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CancelImportTaskResult cancelImportTask() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVolumesResult describeVolumes() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesListingsResult describeReservedInstancesListings()
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeRouteTablesResult describeRouteTables() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeScheduledInstanceAvailabilityResult describeScheduledInstanceAvailability(
            DescribeScheduledInstanceAvailabilityRequest describeScheduledInstanceAvailabilityRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeScheduledInstancesResult describeScheduledInstances(
            DescribeScheduledInstancesRequest describeScheduledInstancesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeDhcpOptionsResult describeDhcpOptions() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrefixListsResult describePrefixLists() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePrincipalIdFormatResult describePrincipalIdFormat(DescribePrincipalIdFormatRequest describePrincipalIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePublicIpv4PoolsResult describePublicIpv4Pools(DescribePublicIpv4PoolsRequest describePublicIpv4PoolsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImportImageTasksResult describeImportImageTasks() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNetworkAclsResult describeNetworkAcls() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeBundleTasksResult describeBundleTasks() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeByoipCidrsResult describeByoipCidrs(DescribeByoipCidrsRequest describeByoipCidrsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCapacityReservationsResult describeCapacityReservations(
        DescribeCapacityReservationsRequest describeCapacityReservationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupIngressResult revokeSecurityGroupIngress(RevokeSecurityGroupIngressRequest revokeSecurityGroupIngressRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public RevokeSecurityGroupIngressResult revokeSecurityGroupIngress() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateInternetGatewayResult createInternetGateway() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeMovingAddressesResult describeMovingAddresses() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeNatGatewaysResult describeNatGateways(DescribeNatGatewaysRequest describeNatGatewaysRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeConversionTasksResult describeConversionTasks() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportImageResult importImage() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFlowLogsResult describeFlowLogs() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeFpgaImageAttributeResult describeFpgaImageAttribute(
        DescribeFpgaImageAttributeRequest describeFpgaImageAttributeRequest) {
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
    public DescribeIamInstanceProfileAssociationsResult describeIamInstanceProfileAssociations(
            DescribeIamInstanceProfileAssociationsRequest describeIamInstanceProfileAssociationsRequest)
            throws AmazonClientException {
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
    public DescribeVpcPeeringConnectionsResult describeVpcPeeringConnections() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribePlacementGroupsResult describePlacementGroups() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSubnetsResult describeSubnets() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstancesResult describeInstances() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeImagesResult describeImages() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotInstanceRequestsResult describeSpotInstanceRequests() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeCustomerGatewaysResult describeCustomerGateways() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkResult describeVpcClassicLink() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcClassicLinkDnsSupportResult describeVpcClassicLinkDnsSupport(
            DescribeVpcClassicLinkDnsSupportRequest describeVpcClassicLinkDnsSupportRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointConnectionNotificationsResult describeVpcEndpointConnectionNotifications(
        DescribeVpcEndpointConnectionNotificationsRequest describeVpcEndpointConnectionNotificationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointConnectionsResult describeVpcEndpointConnections(
        DescribeVpcEndpointConnectionsRequest describeVpcEndpointConnectionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServiceConfigurationsResult describeVpcEndpointServiceConfigurations(
        DescribeVpcEndpointServiceConfigurationsRequest describeVpcEndpointServiceConfigurationsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicePermissionsResult describeVpcEndpointServicePermissions(
        DescribeVpcEndpointServicePermissionsRequest describeVpcEndpointServicePermissionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClassicLinkInstancesResult describeClassicLinkInstances() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClientVpnAuthorizationRulesResult describeClientVpnAuthorizationRules(
        DescribeClientVpnAuthorizationRulesRequest describeClientVpnAuthorizationRulesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClientVpnConnectionsResult describeClientVpnConnections(
        DescribeClientVpnConnectionsRequest describeClientVpnConnectionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClientVpnEndpointsResult describeClientVpnEndpoints(
        DescribeClientVpnEndpointsRequest describeClientVpnEndpointsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClientVpnRoutesResult describeClientVpnRoutes(
        DescribeClientVpnRoutesRequest describeClientVpnRoutesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeClientVpnTargetNetworksResult describeClientVpnTargetNetworks(
        DescribeClientVpnTargetNetworksRequest describeClientVpnTargetNetworksRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTagsResult describeTags() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTransitGatewayAttachmentsResult describeTransitGatewayAttachments(
        DescribeTransitGatewayAttachmentsRequest describeTransitGatewayAttachmentsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTransitGatewayRouteTablesResult describeTransitGatewayRouteTables(
        DescribeTransitGatewayRouteTablesRequest describeTransitGatewayRouteTablesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTransitGatewayVpcAttachmentsResult describeTransitGatewayVpcAttachments(
        DescribeTransitGatewayVpcAttachmentsRequest describeTransitGatewayVpcAttachmentsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeTransitGatewaysResult describeTransitGateways(DescribeTransitGatewaysRequest describeTransitGatewaysRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ImportSnapshotResult importSnapshot() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSpotFleetRequestsResult describeSpotFleetRequests() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesModificationsResult describeReservedInstancesModifications()
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DeleteSpotDatafeedSubscriptionResult deleteSpotDatafeedSubscription() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAddressesResult describeAddresses() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeAggregateIdFormatResult describeAggregateIdFormat(DescribeAggregateIdFormatRequest describeAggregateIdFormatRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeKeyPairsResult describeKeyPairs() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeLaunchTemplateVersionsResult describeLaunchTemplateVersions(
        DescribeLaunchTemplateVersionsRequest describeLaunchTemplateVersionsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeLaunchTemplatesResult describeLaunchTemplates(DescribeLaunchTemplatesRequest describeLaunchTemplatesRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeInstanceStatusResult describeInstanceStatus() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpnGatewaysResult describeVpnGateways() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeReservedInstancesOfferingsResult describeReservedInstancesOfferings()
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointServicesResult describeVpcEndpointServices() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateAddressResult allocateAddress() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public AllocateHostsResult allocateHosts(AllocateHostsRequest allocateHostsRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ApplySecurityGroupsToClientVpnTargetNetworkResult applySecurityGroupsToClientVpnTargetNetwork(
        ApplySecurityGroupsToClientVpnTargetNetworkRequest applySecurityGroupsToClientVpnTargetNetworkRequest) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeSnapshotsResult describeSnapshots() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public DescribeVpcEndpointsResult describeVpcEndpoints() throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public <X extends AmazonWebServiceRequest> DryRunResult<X> dryRun(DryRunSupportedRequest<X> request)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public void shutdown() {
    }

    @Override
    public AmazonEC2Waiters waiters() {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public ModifySpotFleetRequestResult modifySpotFleetRequest(ModifySpotFleetRequestRequest modifySpotFleetRequestRequest)
            throws AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }
}
