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
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.disjoint;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.TAG_PREFIX;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PRIVATE_DNS;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PRIVATE_IP;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PUBLIC_DNS;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PUBLIC_IP;

class AwsEc2UnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private final TransportService transportService;

    private final AmazonEC2 client;

    private final boolean bindAnyGroup;

    private final Set<String> groups;

    private final Map<String, List<String>> tags;

    private final Set<String> availabilityZones;

    private final String hostType;

    private final DiscoNodesCache discoNodes;

    AwsEc2UnicastHostsProvider(Settings settings, TransportService transportService, AwsEc2Service awsEc2Service) {
        super(settings);
        this.transportService = transportService;
        this.client = awsEc2Service.client();

        this.hostType = AwsEc2Service.HOST_TYPE_SETTING.get(settings);
        this.discoNodes = new DiscoNodesCache(AwsEc2Service.NODE_CACHE_TIME_SETTING.get(settings));

        this.bindAnyGroup = AwsEc2Service.ANY_GROUP_SETTING.get(settings);
        this.groups = new HashSet<>();
        this.groups.addAll(AwsEc2Service.GROUPS_SETTING.get(settings));

        this.tags = AwsEc2Service.TAG_SETTING.getAsMap(settings);

        this.availabilityZones = new HashSet<>();
        availabilityZones.addAll(AwsEc2Service.AVAILABILITY_ZONES_SETTING.get(settings));

        if (logger.isDebugEnabled()) {
            logger.debug("using host_type [{}], tags [{}], groups [{}] with any_group [{}], availability_zones [{}]", hostType, tags,
                    groups, bindAnyGroup, availabilityZones);
        }
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        return discoNodes.getOrRefresh();
    }

    protected List<DiscoveryNode> fetchDynamicNodes() {

        List<DiscoveryNode> discoNodes = new ArrayList<>();

        DescribeInstancesResult descInstances;
        try {
            // Query EC2 API based on AZ, instance state, and tag.

            // NOTE: we don't filter by security group during the describe instances request for two reasons:
            // 1. differences in VPCs require different parameters during query (ID vs Name)
            // 2. We want to use two different strategies: (all security groups vs. any security groups)
            descInstances = SocketAccess.doPrivileged(() -> client.describeInstances(buildDescribeInstancesRequest()));
        } catch (AmazonClientException e) {
            logger.info("Exception while retrieving instance list from AWS API: {}", e.getMessage());
            logger.debug("Full exception:", e);
            return discoNodes;
        }

        logger.trace("building dynamic unicast discovery nodes...");
        for (Reservation reservation : descInstances.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                // lets see if we can filter based on groups
                if (!groups.isEmpty()) {
                    List<GroupIdentifier> instanceSecurityGroups = instance.getSecurityGroups();
                    List<String> securityGroupNames = new ArrayList<>(instanceSecurityGroups.size());
                    List<String> securityGroupIds = new ArrayList<>(instanceSecurityGroups.size());
                    for (GroupIdentifier sg : instanceSecurityGroups) {
                        securityGroupNames.add(sg.getGroupName());
                        securityGroupIds.add(sg.getGroupId());
                    }
                    if (bindAnyGroup) {
                        // We check if we can find at least one group name or one group id in groups.
                        if (disjoint(securityGroupNames, groups)
                                && disjoint(securityGroupIds, groups)) {
                            logger.trace("filtering out instance {} based on groups {}, not part of {}", instance.getInstanceId(),
                                    instanceSecurityGroups, groups);
                            // continue to the next instance
                            continue;
                        }
                    } else {
                        // We need tp match all group names or group ids, otherwise we ignore this instance
                        if (!(securityGroupNames.containsAll(groups) || securityGroupIds.containsAll(groups))) {
                            logger.trace("filtering out instance {} based on groups {}, does not include all of {}",
                                    instance.getInstanceId(), instanceSecurityGroups, groups);
                            // continue to the next instance
                            continue;
                        }
                    }
                }

                String address = null;
                if (hostType.equals(PRIVATE_DNS)) {
                    address = instance.getPrivateDnsName();
                } else if (hostType.equals(PRIVATE_IP)) {
                    address = instance.getPrivateIpAddress();
                } else if (hostType.equals(PUBLIC_DNS)) {
                    address = instance.getPublicDnsName();
                } else if (hostType.equals(PUBLIC_IP)) {
                    address = instance.getPublicIpAddress();
                } else if (hostType.startsWith(TAG_PREFIX)) {
                    // Reading the node host from its metadata
                    String tagName = hostType.substring(TAG_PREFIX.length());
                    logger.debug("reading hostname from [{}] instance tag", tagName);
                    List<Tag> tags = instance.getTags();
                    for (Tag tag : tags) {
                        if (tag.getKey().equals(tagName)) {
                            address = tag.getValue();
                            logger.debug("using [{}] as the instance address", address);
                        }
                    }
                } else {
                    throw new IllegalArgumentException(hostType + " is unknown for discovery.ec2.host_type");
                }
                if (address != null) {
                    try {
                        // we only limit to 1 port per address, makes no sense to ping 100 ports
                        TransportAddress[] addresses = transportService.addressesFromString(address, 1);
                        for (int i = 0; i < addresses.length; i++) {
                            logger.trace("adding {}, address {}, transport_address {}", instance.getInstanceId(), address, addresses[i]);
                            discoNodes.add(new DiscoveryNode(instance.getInstanceId(), "#cloud-" + instance.getInstanceId() + "-" + i,
                                addresses[i], emptyMap(), emptySet(), Version.CURRENT.minimumCompatibilityVersion()));
                        }
                    } catch (Exception e) {
                        final String finalAddress = address;
                        logger.warn(
                            (Supplier<?>)
                                () -> new ParameterizedMessage("failed to add {}, address {}", instance.getInstanceId(), finalAddress), e);
                    }
                } else {
                    logger.trace("not adding {}, address is null, host_type {}", instance.getInstanceId(), hostType);
                }
            }
        }

        logger.debug("using dynamic discovery nodes {}", discoNodes);

        return discoNodes;
    }

    private DescribeInstancesRequest buildDescribeInstancesRequest() {
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
            .withFilters(
                new Filter("instance-state-name").withValues("running", "pending")
            );

        for (Map.Entry<String, List<String>> tagFilter : tags.entrySet()) {
            // for a given tag key, OR relationship for multiple different values
            describeInstancesRequest.withFilters(
                new Filter("tag:" + tagFilter.getKey()).withValues(tagFilter.getValue())
            );
        }

        if (!availabilityZones.isEmpty()) {
            // OR relationship amongst multiple values of the availability-zone filter
            describeInstancesRequest.withFilters(
                new Filter("availability-zone").withValues(availabilityZones)
            );
        }

        return describeInstancesRequest;
    }

    private final class DiscoNodesCache extends SingleObjectCache<List<DiscoveryNode>> {

        private boolean empty = true;

        protected DiscoNodesCache(TimeValue refreshInterval) {
            super(refreshInterval,  new ArrayList<>());
        }

        @Override
        protected boolean needsRefresh() {
            return (empty || super.needsRefresh());
        }

        @Override
        protected List<DiscoveryNode> refresh() {
            List<DiscoveryNode> nodes = fetchDynamicNodes();
            empty = nodes.isEmpty();
            return nodes;
        }
    }
}
