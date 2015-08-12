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
import com.amazonaws.services.ec2.model.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.*;
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
    public void resetInstanceAttribute(ResetInstanceAttributeRequest resetInstanceAttributeRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("Not supported in mock");
    }

    @Override
    public CreateKeyPairResult createKeyPair(CreateKeyPairRequest createKeyPairRequest) throws AmazonServiceException, AmazonClientException {
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
}
