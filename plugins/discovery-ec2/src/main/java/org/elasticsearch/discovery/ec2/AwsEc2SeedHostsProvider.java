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
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.disjoint;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PRIVATE_DNS;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PRIVATE_IP;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PUBLIC_DNS;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.PUBLIC_IP;
import static org.elasticsearch.discovery.ec2.AwsEc2Service.HostType.TAG_PREFIX;

class AwsEc2SeedHostsProvider implements SeedHostsProvider {

    private static final Logger logger = LogManager.getLogger(AwsEc2SeedHostsProvider.class);

    private final TransportService transportService;

    private final AwsEc2Service awsEc2Service;

    private final boolean bindAnyGroup;

    private final Set<String> groups;

    private final Map<String, List<String>> tags;

    private final Set<String> availabilityZones;

    private final String hostType;

    private final TransportAddressesCache dynamicHosts;

    AwsEc2SeedHostsProvider(Settings settings, TransportService transportService, AwsEc2Service awsEc2Service) {
        this.transportService = transportService;
        this.awsEc2Service = awsEc2Service;

        this.hostType = AwsEc2Service.HOST_TYPE_SETTING.get(settings);
        this.dynamicHosts = new TransportAddressesCache(AwsEc2Service.NODE_CACHE_TIME_SETTING.get(settings));

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
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        return dynamicHosts.getOrRefresh();
    }

    protected List<TransportAddress> fetchDynamicNodes() {

        final List<TransportAddress> dynamicHosts = new ArrayList<>();

        final DescribeInstancesResult descInstances;
        try (AmazonEc2Reference clientReference = awsEc2Service.client()) {
            // Query EC2 API based on AZ, instance state, and tag.

            // NOTE: we don't filter by security group during the describe instances request for two reasons:
            // 1. differences in VPCs require different parameters during query (ID vs Name)
            // 2. We want to use two different strategies: (all security groups vs. any security groups)
            descInstances = SocketAccess.doPrivileged(() -> clientReference.client().describeInstances(buildDescribeInstancesRequest()));
        } catch (final AmazonClientException e) {
            logger.info("Exception while retrieving instance list from AWS API: {}", e.getMessage());
            logger.debug("Full exception:", e);
            return dynamicHosts;
        }

        logger.trace("finding seed nodes...");
        for (final Reservation reservation : descInstances.getReservations()) {
            for (final Instance instance : reservation.getInstances()) {
                // lets see if we can filter based on groups
                if (!groups.isEmpty()) {
                    final List<GroupIdentifier> instanceSecurityGroups = instance.getSecurityGroups();
                    final List<String> securityGroupNames = new ArrayList<>(instanceSecurityGroups.size());
                    final List<String> securityGroupIds = new ArrayList<>(instanceSecurityGroups.size());
                    for (final GroupIdentifier sg : instanceSecurityGroups) {
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
                    final String tagName = hostType.substring(TAG_PREFIX.length());
                    logger.debug("reading hostname from [{}] instance tag", tagName);
                    final List<Tag> tags = instance.getTags();
                    for (final Tag tag : tags) {
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
                        final TransportAddress[] addresses = transportService.addressesFromString(address);
                        for (int i = 0; i < addresses.length; i++) {
                            logger.trace("adding {}, address {}, transport_address {}", instance.getInstanceId(), address, addresses[i]);
                            dynamicHosts.add(addresses[i]);
                        }
                    } catch (final Exception e) {
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

        logger.debug("using dynamic transport addresses {}", dynamicHosts);

        return dynamicHosts;
    }

    private DescribeInstancesRequest buildDescribeInstancesRequest() {
        final DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
            .withFilters(
                new Filter("instance-state-name").withValues("running", "pending")
            );

        for (final Map.Entry<String, List<String>> tagFilter : tags.entrySet()) {
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

    private final class TransportAddressesCache extends SingleObjectCache<List<TransportAddress>> {

        protected TransportAddressesCache(TimeValue refreshInterval) {
            super(refreshInterval,  new ArrayList<>());
        }

        @Override
        protected List<TransportAddress> refresh() {
            return fetchDynamicNodes();
        }
    }
}
