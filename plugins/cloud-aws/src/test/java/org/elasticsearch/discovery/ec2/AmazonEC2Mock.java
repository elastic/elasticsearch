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
