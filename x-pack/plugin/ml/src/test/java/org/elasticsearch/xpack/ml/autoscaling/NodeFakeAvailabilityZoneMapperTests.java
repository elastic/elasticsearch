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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class NodeFakeAvailabilityZoneMapperTests extends ESTestCase {

    public void testBeforeClusterReady() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        NodeFakeAvailabilityZoneMapper nodeFakeAvailabilityZoneMapper = new NodeFakeAvailabilityZoneMapper(settings, clusterSettings);

        assertThat(nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), anEmptyMap());
        assertThat(nodeFakeAvailabilityZoneMapper.getNumAvailabilityZones(), is(OptionalInt.empty()));
    }

    public void testAvailabilityZonesAttributesNotConfiguredMultiRoleNodes() {
        // Fake availability zones are populated completely independently of cluster settings,
        // attributes etc. Here we test that even in the absence of zone attributes that the
        // fake availability zone mapper still assigns each node to a single, unique availability
        // zone
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        DiscoveryNode node1 = DiscoveryNodeUtils.create(
            "node-1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(MASTER_ROLE, DATA_ROLE, ML_ROLE)
        );

        DiscoveryNode node2 = DiscoveryNodeUtils.create(
            "node-2",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
            Map.of(),
            Set.of(MASTER_ROLE, DATA_ROLE, ML_ROLE)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).build();

        NodeFakeAvailabilityZoneMapper nodeFakeAvailabilityZoneMapper = new NodeFakeAvailabilityZoneMapper(
            settings,
            clusterSettings,
            discoveryNodes
        );

        DiscoveryNodes expectedDiscoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        assertThat(nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(2));
        for (DiscoveryNode node : expectedDiscoveryNodes) {
            assertThat(
                nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().get(List.of(node.getId())),
                equalTo(new ArrayList<DiscoveryNode>(List.of(node)))
            );
        }
        assertThat(nodeFakeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(2));

        DiscoveryNodes expectedMlDiscoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        assertThat(nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone(), aMapWithSize(2));
        for (DiscoveryNode node : expectedMlDiscoveryNodes) {
            assertThat(
                nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().get(List.of(node.getId())),
                equalTo(new ArrayList<DiscoveryNode>(List.of(node)))
            );
        }
        assertThat(nodeFakeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(2));
    }

    public void testAvailabilityZonesAttributesNotConfiguredDedicatedNodes() {
        // Fake availability zones are populated completely independently of cluster settings,
        // attributes etc. Here we test that even in the absence of zone attributes that the
        // fake availability zone mapper still assigns each node to a single, unique availability
        // zone, and that dedicated ML nodes are identified and allocated to ML availability zones.
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        DiscoveryNode mlNode = DiscoveryNodeUtils.create(
            "node-1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(ML_ROLE)
        );
        DiscoveryNode node1 = DiscoveryNodeUtils.create(
            "node-2",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
            Map.of(),
            Set.of(MASTER_ROLE)
        );
        DiscoveryNode node2 = DiscoveryNodeUtils.create(
            "node-3",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9202),
            Map.of(),
            Set.of(DATA_ROLE)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(mlNode).add(node1).add(node2).build();
        NodeFakeAvailabilityZoneMapper nodeFakeAvailabilityZoneMapper = new NodeFakeAvailabilityZoneMapper(
            settings,
            clusterSettings,
            discoveryNodes
        );

        DiscoveryNodes expectedDiscoveryNodes = DiscoveryNodes.builder().add(mlNode).add(node1).add(node2).build();
        assertThat(nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(3));
        for (DiscoveryNode node : expectedDiscoveryNodes) {
            assertThat(
                nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone().get(List.of(node.getId())),
                equalTo(new ArrayList<DiscoveryNode>(List.of(node)))
            );
        }
        assertThat(nodeFakeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(3));

        DiscoveryNodes expectedMlDiscoveryNodes = DiscoveryNodes.builder().add(mlNode).build();

        assertThat(nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone(), aMapWithSize(1));
        assertThat(nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone().get(List.of("node-1")), contains(mlNode));
        assertThat(nodeFakeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(1));
    }

    public void testAvailabilityZonesAttributesConfiguredMultiRoleNodes() {
        // Fake availability zones are populated completely independently of cluster settings,
        // attributes etc. Here we test that the fake availability zone mapper assigns each node
        // to a single, unique availability zone, even when explicit allocation awareness attributes are
        // configured in the cluster settings.
        Settings settings = Settings.builder()
            .putList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                List.of("region", "logical_availability_zone")
            )
            .build();

        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(2, 50);

        // For fake availability zones, each node is mapped to a unique zone, i.e. the number of zones is equal to the number of nodes.
        // This allocation of zones is performed completely independently of cluster settings, attributes etc.
        int numZones = numNodes;
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
        NodeFakeAvailabilityZoneMapper nodeFakeAvailabilityZoneMapper = new NodeFakeAvailabilityZoneMapper(
            settings,
            clusterSettings,
            discoveryNodes
        );

        assertThat(nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(numNodes));
        int totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(1));
            assertThat(entry.getValue().size(), is(1));
            assertThat(key.get(0), equalTo(entry.getValue().iterator().next().getId()));

            ++totalNodesMapped;
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeFakeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(numZones));
        totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(1));
            assertThat(entry.getValue().size(), is(1));
            assertThat(key.get(0), equalTo(entry.getValue().iterator().next().getId()));
            String zoneAttributeValue = key.get(0);
            ++totalNodesMapped;
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeFakeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(numZones));
    }

    public void testAvailabilityZonesAttributesConfiguredDedicatedNodes() {
        // Fake availability zones are populated completely independently of cluster settings,
        // attributes etc. Here we test that the fake availability zone mapper assigns each node
        // to a single, unique availability zone, even when explicit allocation awareness attributes are
        // configured in the cluster settings, and that dedicated ML nodes are identified and allocated
        // to ML availability zones.
        Settings settings = Settings.builder()
            .putList(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                List.of("region", "logical_availability_zone")
            )
            .build();
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        List<DiscoveryNode> mlNodes = new ArrayList<>();
        Set<Integer> mlZones = new HashSet<>();
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(10, 50);
        int numZones = numNodes;
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
        NodeFakeAvailabilityZoneMapper nodeFakeAvailabilityZoneMapper = new NodeFakeAvailabilityZoneMapper(
            settings,
            clusterSettings,
            discoveryNodes
        );

        assertThat(nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone(), aMapWithSize(numZones));
        int totalNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeFakeAvailabilityZoneMapper.getAllNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();

            assertThat(key, hasSize(1));
            assertThat(entry.getValue().size(), is(1));
            assertThat(key.get(0), equalTo(entry.getValue().iterator().next().getId()));
            ++totalNodesMapped;
        }
        assertThat(totalNodesMapped, is(numNodes));
        assertThat(nodeFakeAvailabilityZoneMapper.getNumAvailabilityZones().getAsInt(), is(numZones));
        int totalMlNodesMapped = 0;
        for (Map.Entry<List<String>, Collection<DiscoveryNode>> entry : nodeFakeAvailabilityZoneMapper.getMlNodesByAvailabilityZone()
            .entrySet()) {
            List<String> key = entry.getKey();
            assertThat(key, hasSize(1));
            assertThat(entry.getValue().size(), is(1));
            assertThat(key.get(0), equalTo(entry.getValue().iterator().next().getId()));
            ++totalMlNodesMapped;
        }
        assertThat(totalMlNodesMapped, is(mlNodes.size()));
        assertThat(nodeFakeAvailabilityZoneMapper.getNumMlAvailabilityZones().getAsInt(), is(mlZones.size()));
    }
}
