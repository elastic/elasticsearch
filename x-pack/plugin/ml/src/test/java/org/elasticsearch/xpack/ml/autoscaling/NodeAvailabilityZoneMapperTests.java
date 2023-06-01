/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.ML_ROLE;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class NodeAvailabilityZoneMapperTests extends ESTestCase {

    public void testBeforeClusterReady() {
        Settings settings = Settings.builder()
            .putList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                List.of("region", "logical_availability_zone")
            )
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );

        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(settings, clusterSettings);

        assertThat(nodeAvailabilityZoneMapper.getAwarenessAttributes(), equalTo(List.of("region", "logical_availability_zone")));
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), anEmptyMap());
        assertThat(nodeAvailabilityZoneMapper.getNumAvailabilityZones(), is(OptionalInt.empty()));
    }

    public void testAvailabilityZonesNotConfiguredMultiRoleNodes() {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "node-1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Map.of(),
                    Set.of(MASTER_ROLE, DATA_ROLE, ML_ROLE)
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Map.of(),
                    Set.of(MASTER_ROLE, DATA_ROLE, ML_ROLE)
                )
            )
            .build();
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes);

        assertThat(nodeAvailabilityZoneMapper.getAwarenessAttributes(), empty());
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(1));
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().keySet().iterator().next(), empty());
        assertThat(
            nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().get(List.of()),
            containsInAnyOrder(discoveryNodes.getNodes().values().toArray())
        );
        assertThat(nodeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(1));
        assertThat(nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone(), aMapWithSize(1));
        assertThat(nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().keySet().iterator().next(), empty());
        assertThat(
            nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().get(List.of()),
            containsInAnyOrder(discoveryNodes.getNodes().values().toArray())
        );
        assertThat(nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(1));
    }

    public void testAvailabilityZonesNotConfiguredDedicatedNodes() {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );

        DiscoveryNode mlNode = DiscoveryNodeUtils.create(
            "node-1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(ML_ROLE)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(mlNode)
            .add(
                DiscoveryNodeUtils.create(
                    "node-2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Map.of(),
                    Set.of(MASTER_ROLE)
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9202),
                    Map.of(),
                    Set.of(DATA_ROLE)
                )
            )
            .build();
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes);

        assertThat(nodeAvailabilityZoneMapper.getAwarenessAttributes(), empty());
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(1));
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().keySet().iterator().next(), empty());
        assertThat(
            nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().get(List.of()),
            containsInAnyOrder(discoveryNodes.getNodes().values().toArray())
        );
        assertThat(nodeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(1));
        assertThat(nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone(), aMapWithSize(1));
        assertThat(nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().keySet().iterator().next(), empty());
        assertThat(nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().get(List.of()), contains(mlNode));
        assertThat(nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(1));
    }

    public void testAvailabilityZonesConfiguredMultiRoleNodes() {
        Settings settings = Settings.builder()
            .putList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                List.of("region", "logical_availability_zone")
            )
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(2, 50);
        int numZones = randomIntBetween(1, Math.min(numNodes, 3));
        for (int nodeNum = 1; nodeNum <= numNodes; ++nodeNum) {
            discoveryNodesBuilder.add(
                DiscoveryNodeUtils.create(
                    "node-" + nodeNum,
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9299 + nodeNum),
                    Map.of("region", "unknown-region", "logical_availability_zone", "zone-" + (nodeNum % numZones)),
                    Set.of(MASTER_ROLE, DATA_ROLE, ML_ROLE)
                )
            );
        }

        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes);

        assertThat(nodeAvailabilityZoneMapper.getAwarenessAttributes(), equalTo(List.of("region", "logical_availability_zone")));
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(numZones));
        int totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(2));
            assertThat(key.get(0), equalTo("unknown-region"));
            String zoneAttributeValue = key.get(1);
            for (DiscoveryNode node : entry.getValue()) {
                assertThat(node.getAttributes().get("logical_availability_zone"), equalTo(zoneAttributeValue));
                ++totalNodesMapped;
            }
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(numZones));
        totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(2));
            assertThat(key.get(0), equalTo("unknown-region"));
            String zoneAttributeValue = key.get(1);
            for (DiscoveryNode node : entry.getValue()) {
                assertThat(node.getAttributes().get("logical_availability_zone"), equalTo(zoneAttributeValue));
                ++totalNodesMapped;
            }
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(numZones));
    }

    public void testAvailabilityZonesConfiguredDedicatedNodes() {
        Settings settings = Settings.builder()
            .putList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                List.of("region", "logical_availability_zone")
            )
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );

        List<DiscoveryNode> mlNodes = new ArrayList<>();
        Set<Integer> mlZones = new HashSet<>();
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(10, 50);
        int numZones = randomIntBetween(2, 3);
        int numMlZones = randomIntBetween(1, numZones);
        for (int nodeNum = 1; nodeNum <= numNodes; ++nodeNum) {
            int zone = nodeNum % numZones;
            DiscoveryNodeRole role = (zone < numMlZones) ? randomFrom(MASTER_ROLE, DATA_ROLE, ML_ROLE) : randomFrom(MASTER_ROLE, DATA_ROLE);
            DiscoveryNode node = DiscoveryNodeUtils.create(
                "node-" + nodeNum,
                new TransportAddress(InetAddress.getLoopbackAddress(), 9199 + nodeNum),
                Map.of("region", "unknown-region", "logical_availability_zone", "zone-" + zone),
                Set.of(role)
            );
            if (role == ML_ROLE) {
                mlNodes.add(node);
                mlZones.add(zone);
            }
            discoveryNodesBuilder.add(node);
        }

        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper = new NodeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes);

        assertThat(nodeAvailabilityZoneMapper.getAwarenessAttributes(), equalTo(List.of("region", "logical_availability_zone")));
        assertThat(nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(numZones));
        int totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeAvailabilityZoneMapper.getAllNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(2));
            assertThat(key.get(0), equalTo("unknown-region"));
            String zoneAttributeValue = key.get(1);
            for (DiscoveryNode node : entry.getValue()) {
                assertThat(node.getAttributes().get("logical_availability_zone"), equalTo(zoneAttributeValue));
                ++totalNodesMapped;
            }
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(numZones));
        int totalMlNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(2));
            assertThat(key.get(0), equalTo("unknown-region"));
            String zoneAttributeValue = key.get(1);
            for (DiscoveryNode node : entry.getValue()) {
                assertThat(node.getAttributes().get("logical_availability_zone"), equalTo(zoneAttributeValue));
                ++totalMlNodesMapped;
            }
        }
        assertThat(totalMlNodesMapped, is(mlNodes.size()));
        assertThat(nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(mlZones.size()));
    }
}
