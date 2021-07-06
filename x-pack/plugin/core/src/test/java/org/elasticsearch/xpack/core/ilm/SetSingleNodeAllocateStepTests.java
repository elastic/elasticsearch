/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.times;

public class SetSingleNodeAllocateStepTests extends AbstractStepTestCase<SetSingleNodeAllocateStep> {

    @Override
    protected SetSingleNodeAllocateStep createRandomInstance() {
        return new SetSingleNodeAllocateStep(randomStepKey(), randomStepKey(), client);
    }

    @Override
    protected SetSingleNodeAllocateStep mutateInstance(SetSingleNodeAllocateStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new SetSingleNodeAllocateStep(key, nextKey, instance.getClient());
    }

    @Override
    protected SetSingleNodeAllocateStep copyInstance(SetSingleNodeAllocateStep instance) {
        return new SetSingleNodeAllocateStep(instance.getKey(), instance.getNextStepKey(), client);
    }

    public static void assertSettingsRequestContainsValueFrom(UpdateSettingsRequest request, String settingsKey,
                                                              Set<String> acceptableValues, boolean assertOnlyKeyInSettings,
                                                              String... expectedIndices) {
        assertNotNull(request);
        assertArrayEquals(expectedIndices, request.indices());
        assertThat(request.settings().get(settingsKey), anyOf(acceptableValues.stream().map(e -> equalTo(e)).collect(Collectors.toList())));
        if (assertOnlyKeyInSettings) {
            assertEquals(1, request.settings().size());
        }
    }

    public void testPerformActionNoAttrs() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Settings nodeSettings = Settings.builder().put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName).build();
            nodes.add(
                    DiscoveryNode.createLocal(nodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, nodePort), nodeId));
            validNodeIds.add(nodeId);
        }

        assertNodeSelected(indexMetadata, index, validNodeIds, nodes);
    }

    public void testPerformActionAttrsAllNodesValid() throws IOException {
        int numAttrs = randomIntBetween(1, 10);
        final int numNodes = randomIntBetween(1, 20);
        String[][] validAttrs = new String[numAttrs][2];
        for (int i = 0; i < numAttrs; i++) {
            validAttrs[i] = new String[] { randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20) };
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        for (String[] attr : validAttrs) {
            indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attr[0], attr[1]);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            String[] nodeAttr = randomFrom(validAttrs);
            Settings nodeSettings = Settings.builder().put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                    .put(Node.NODE_ATTRIBUTES.getKey() + nodeAttr[0], nodeAttr[1]).build();
            nodes.add(DiscoveryNode.createLocal(nodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, nodePort), nodeId));
            validNodeIds.add(nodeId);
        }

        assertNodeSelected(indexMetadata, index, validNodeIds, nodes);
    }

    public void testPerformActionAttrsSomeNodesValid() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        String[] validAttr = new String[] { "box_type", "valid" };
        String[] invalidAttr = new String[] { "box_type", "not_valid" };
        Settings.Builder indexSettings = settings(Version.CURRENT);
        indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + validAttr[0], validAttr[1]);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + validAttr[0], validAttr[1]).build();
        Settings invalidNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + invalidAttr[0], invalidAttr[1]).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder();
            // randomise whether the node had valid attributes or not but make sure at least one node is valid
            if (randomBoolean() || (i == numNodes - 1 && validNodeIds.isEmpty())) {
                nodeSettingsBuilder.put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName);
                validNodeIds.add(nodeId);
            } else {
                nodeSettingsBuilder.put(invalidNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName);
            }
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                    nodeId));
        }

        assertNodeSelected(indexMetadata, index, validNodeIds, nodes);
    }

    public void testPerformActionWithSomeNodesHasNoEnoughDiskBytes() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        Settings.Builder indexSettings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Set<String> validNodeIds = new HashSet<>();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder();
            // randomise whether the node's disk had enough available bytes or not but make sure at least one node is valid
            if (randomBoolean() || (i == numNodes - 1 && validNodeIds.isEmpty())) {
                validNodeIds.add(nodeId);
            }
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                nodeId));
        }

        assertNodeSelected(indexMetadata, index, validNodeIds, nodes);
    }

    public void testPerformActionWithAllNodesHasNoEnoughDiskBytes() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        Settings.Builder indexSettings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder();
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                nodeId));
        }
        DiscoveryNodes discoveryNodes = nodes.build();

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);

        IndexRoutingTable indexRoutingTable = createRoutingTable(indexMetadata, index, discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().indices(indices.build()))
            .nodes(discoveryNodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        List<NodeStats> nodeStats = mockNodeStats(index, discoveryNodes, indexRoutingTable, Collections.emptySet());
        mockNodeStatsCall(nodeStats);

        expectThrows(NoNodeAvailableException.class,
            () -> PlainActionFuture.<Boolean, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f)));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).cluster();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
    }

    public void testPerformActionNodeContainsMaximumShardsStorageBytesSelected() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        Settings.Builder indexSettings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(0).build();
        Index index = indexMetadata.getIndex();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        List<String> validNodeIds = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder();
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                nodeId));
            validNodeIds.add(nodeId);

        }
        DiscoveryNodes discoveryNodes = nodes.build();

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int primary = 0; primary < indexMetadata.getNumberOfShards(); primary++) {
            // node_id_0 will be selected because it contains maximum shards storage bytes
            String currentNode = "node_id_0";
            indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, primary), currentNode,
                true, ShardRoutingState.STARTED));
        }
        IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().indices(indices.build()))
            .nodes(discoveryNodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        List<NodeStats> nodeStats = mockNodeStats(index, discoveryNodes, indexRoutingTable, new HashSet<>(validNodeIds));
        mockNodeStatsCall(nodeStats);

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertSettingsRequestContainsValueFrom(request,
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", Set.of("node_id_0"), true,
                indexMetadata.getIndex().getName());
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));

        Mockito.verify(client, times(2)).admin();
        Mockito.verify(adminClient, times(1)).cluster();
        Mockito.verify(adminClient, times(1)).indices();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionGetNodeStatsFail() throws IOException {
        final int numNodes = randomIntBetween(1, 20);
        Settings.Builder indexSettings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder();
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                nodeId));
        }
        DiscoveryNodes discoveryNodes = nodes.build();

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);

        IndexRoutingTable indexRoutingTable = createRoutingTable(indexMetadata, index, discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().indices(indices.build()))
            .nodes(discoveryNodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        Exception exception = new RuntimeException();

        Mockito.doAnswer(invocation -> {
            NodesStatsRequest request = (NodesStatsRequest) invocation.getArguments()[0];
            assertThat(request.requestedMetrics().size(), equalTo(1));
            assertThat(request.indices().isSet(CommonStatsFlags.Flag.Store), equalTo(true));
            @SuppressWarnings("unchecked")
            ActionListener<NodesStatsResponse> listener = (ActionListener<NodesStatsResponse>) invocation.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(clusterClient).nodesStats(Mockito.any(), Mockito.any());

        SetSingleNodeAllocateStep step = createRandomInstance();

        assertSame(exception, expectThrows(Exception.class, () -> PlainActionFuture.<Boolean, Exception>get(
            f -> step.performAction(indexMetadata, clusterState, null, f))));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).cluster();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
    }

    public void testPerformActionWithClusterExcludeFilters() throws IOException {
        Settings.Builder indexSettings = settings(Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 1)).build();
        Index index = indexMetadata.getIndex();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        String nodeId = "node_id_0";
        int nodePort = 9300;
        Builder nodeSettingsBuilder = Settings.builder();
        nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
            nodeId));
        DiscoveryNodes discoveryNodes = nodes.build();

        Settings clusterSettings = Settings.builder()
            .put("cluster.routing.allocation.exclude._id", "node_id_0")
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node_id_0", true, ShardRoutingState.STARTED)).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().indices(indices.build()).transientSettings(clusterSettings))
            .nodes(discoveryNodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        List<NodeStats> nodeStats = mockNodeStats(index, discoveryNodes, indexRoutingTable, Set.of(nodeId));
        mockNodeStatsCall(nodeStats);

        expectThrows(NoNodeAvailableException.class,
            () -> PlainActionFuture.<Boolean, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f)));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).cluster();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
    }

    public void testPerformActionAttrsNoNodesValid() {
        final int numNodes = randomIntBetween(1, 20);
        String[] validAttr = new String[] { "box_type", "valid" };
        String[] invalidAttr = new String[] { "box_type", "not_valid" };
        Settings.Builder indexSettings = settings(Version.CURRENT);
        indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + validAttr[0], validAttr[1]);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Settings invalidNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + invalidAttr[0], invalidAttr[1]).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder().put(invalidNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName);
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                    nodeId));
        }

        assertNoValidNode(indexMetadata, index, nodes);
    }

    public void testPerformActionAttrsRequestFails() {
        final int numNodes = randomIntBetween(1, 20);
        int numAttrs = randomIntBetween(1, 10);
        Map<String, String> validAttributes = new HashMap<>();
        for (int i = 0; i < numAttrs; i++) {
            validAttributes.put(randomValueOtherThanMany(validAttributes::containsKey,
                () -> randomAlphaOfLengthBetween(1, 20)), randomAlphaOfLengthBetween(1, 20));
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        validAttributes.forEach((k, v) -> {
            indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);

        });
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Map.Entry<String, String> nodeAttr = randomFrom(validAttributes.entrySet());
            Settings nodeSettings = Settings.builder().put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                .put(Node.NODE_ATTRIBUTES.getKey() + nodeAttr.getKey(), nodeAttr.getValue()).build();
            nodes.add(DiscoveryNode.createLocal(nodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, nodePort), nodeId));
            validNodeIds.add(nodeId);
        }

        DiscoveryNodes discoveryNodes = nodes.build();

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata> builder().fPut(index.getName(),
            indexMetadata);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node_id_0", true, ShardRoutingState.STARTED)).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices.build()))
            .nodes(discoveryNodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        List<NodeStats> nodeStats = mockNodeStats(index, discoveryNodes, indexRoutingTable, validNodeIds);
        mockNodeStatsCall(nodeStats);

        Exception exception = new RuntimeException();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertSettingsRequestContainsValueFrom(request,
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", validNodeIds, true,
                indexMetadata.getIndex().getName());
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        assertSame(exception, expectThrows(Exception.class, () -> PlainActionFuture.<Boolean, Exception>get(
            f -> step.performAction(indexMetadata, clusterState, null, f))));

        Mockito.verify(client, times(2)).admin();
        Mockito.verify(adminClient, times(1)).cluster();
        Mockito.verify(adminClient, times(1)).indices();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionAttrsNoShard() {
        int numAttrs = randomIntBetween(1, 10);
        final int numNodes = randomIntBetween(1, 20);
        String[][] validAttrs = new String[numAttrs][2];
        for (int i = 0; i < numAttrs; i++) {
            validAttrs[i] = new String[] { "na_" + randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20) };
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        for (String[] attr : validAttrs) {
            indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attr[0], attr[1]);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, numNodes - 1)).build();
        Index index = indexMetadata.getIndex();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            String[] nodeAttr = randomFrom(validAttrs);
            Settings nodeSettings = Settings.builder().put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                    .put(Node.NODE_ATTRIBUTES.getKey() + nodeAttr[0], nodeAttr[1]).build();
            nodes.add(DiscoveryNode.createLocal(nodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, nodePort), nodeId));
        }

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata> builder().fPut(index.getName(),
                indexMetadata);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices.build()))
                .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
            () -> PlainActionFuture.<Boolean, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f)));
        assertEquals(indexMetadata.getIndex(), e.getIndex());

        Mockito.verifyZeroInteractions(client);
    }

    public void testPerformActionSomeShardsOnlyOnNewNodes() {
        final Version oldVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.CURRENT);
        final int numNodes = randomIntBetween(2, 20); // Need at least 2 nodes to have some nodes on a new version
        final int numNewNodes = randomIntBetween(1, numNodes - 1);
        final int numOldNodes = numNodes - numNewNodes;

        final int numberOfShards = randomIntBetween(1, 5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(oldVersion))
            .numberOfShards(numberOfShards).numberOfReplicas(randomIntBetween(0, numNewNodes - 1)).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();

        Set<String> newNodeIds = new HashSet<>();
        for (int i = 0; i < numNewNodes; i++) {
            String nodeId = "new_node_id_" + i;
            String nodeName = "new_node_" + i;
            int nodePort = 9300 + i;
            Settings nodeSettings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build();
            newNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                Version.CURRENT));
        }

        Set<String> oldNodeIds = new HashSet<>();
        for (int i = 0; i < numOldNodes; i++) {
            String nodeId = "old_node_id_" + i;
            String nodeName = "old_node_" + i;
            int nodePort = 9300 + numNewNodes + i;
            Settings nodeSettings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build();
            oldNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                oldVersion));
        }

        Set<String> nodeIds = new HashSet<>();
        nodeIds.addAll(newNodeIds);
        nodeIds.addAll(oldNodeIds);

        DiscoveryNodes discoveryNodes = nodes.build();
        IndexRoutingTable.Builder indexRoutingTable = createRoutingTableWithOneShardOnSubset(indexMetadata, newNodeIds, nodeIds);

        // Since one shard is already on only new nodes, we should always pick a new node
        assertNodeSelected(indexMetadata, indexMetadata.getIndex(), newNodeIds, discoveryNodes, indexRoutingTable.build());
    }

    public void testPerformActionSomeShardsOnlyOnNewNodesButNewNodesInvalidAttrs() {
        final Version oldVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.CURRENT);
        final int numNodes = randomIntBetween(2, 20); // Need at least 2 nodes to have some nodes on a new version
        final int numNewNodes = randomIntBetween(1, numNodes - 1);
        final int numOldNodes = numNodes - numNewNodes;
        final int numberOfShards = randomIntBetween(1, 5);
        final String attribute = "box_type";
        final String validAttr = "valid";
        final String invalidAttr = "not_valid";
        Settings.Builder indexSettings = settings(oldVersion);
        indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attribute, validAttr);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(numberOfShards).numberOfReplicas(randomIntBetween(0, numNewNodes - 1)).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();

        Set<String> newNodeIds = new HashSet<>();
        for (int i = 0; i < numNewNodes; i++) {
            String nodeId = "new_node_id_" + i;
            String nodeName = "new_node_" + i;
            int nodePort = 9300 + i;
            Settings nodeSettings = Settings.builder()
                .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                .put(Node.NODE_ATTRIBUTES.getKey() + attribute, invalidAttr).build();
            newNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                Version.CURRENT));
        }

        Set<String> oldNodeIds = new HashSet<>();
        for (int i = 0; i < numOldNodes; i++) {
            String nodeId = "old_node_id_" + i;
            String nodeName = "old_node_" + i;
            int nodePort = 9300 + numNewNodes + i;
            Settings nodeSettings = Settings.builder()
                .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                .put(Node.NODE_ATTRIBUTES.getKey() + attribute, validAttr).build();
            oldNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                oldVersion));
        }
        Set<String> nodeIds = new HashSet<>();
        nodeIds.addAll(newNodeIds);
        nodeIds.addAll(oldNodeIds);

        DiscoveryNodes discoveryNodes = nodes.build();
        IndexRoutingTable.Builder indexRoutingTable = createRoutingTableWithOneShardOnSubset(indexMetadata, newNodeIds, nodeIds);

        assertNoValidNode(indexMetadata, indexMetadata.getIndex(), discoveryNodes, indexRoutingTable.build());
    }

    public void testPerformActionNewShardsExistButWithInvalidAttributes() {
        final Version oldVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.CURRENT);
        final int numNodes = randomIntBetween(2, 20); // Need at least 2 nodes to have some nodes on a new version
        final int numNewNodes = randomIntBetween(1, numNodes - 1);
        final int numOldNodes = numNodes - numNewNodes;
        final int numberOfShards = randomIntBetween(1, 5);
        final String attribute = "box_type";
        final String validAttr = "valid";
        final String invalidAttr = "not_valid";
        Settings.Builder indexSettings = settings(oldVersion);
        indexSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attribute, validAttr);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(numberOfShards).numberOfReplicas(randomIntBetween(0, numOldNodes - 1)).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();

        Set<String> newNodeIds = new HashSet<>();
        for (int i = 0; i < numNewNodes; i++) {
            String nodeId = "new_node_id_" + i;
            String nodeName = "new_node_" + i;
            int nodePort = 9300 + i;
            Settings nodeSettings = Settings.builder()
                .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                .put(Node.NODE_ATTRIBUTES.getKey() + attribute, invalidAttr).build();
            newNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                Version.CURRENT));
        }

        Set<String> oldNodeIds = new HashSet<>();
        for (int i = 0; i < numOldNodes; i++) {
            String nodeId = "old_node_id_" + i;
            String nodeName = "old_node_" + i;
            int nodePort = 9300 + numNewNodes + i;
            Settings nodeSettings = Settings.builder()
                .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
                .put(Node.NODE_ATTRIBUTES.getKey() + attribute, validAttr).build();
            oldNodeIds.add(nodeId);
            nodes.add(new DiscoveryNode(
                Node.NODE_NAME_SETTING.get(nodeSettings),
                nodeId,
                new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                Node.NODE_ATTRIBUTES.getAsMap(nodeSettings),
                DiscoveryNode.getRolesFromSettings(nodeSettings),
                oldVersion));
        }
        Set<String> nodeIds = new HashSet<>();
        nodeIds.addAll(newNodeIds);
        nodeIds.addAll(oldNodeIds);

        DiscoveryNodes discoveryNodes = nodes.build();
        IndexRoutingTable.Builder indexRoutingTable = createRoutingTableWithOneShardOnSubset(indexMetadata, oldNodeIds, oldNodeIds);

        assertNodeSelected(indexMetadata, indexMetadata.getIndex(), oldNodeIds, discoveryNodes, indexRoutingTable.build());
    }

    private void assertNodeSelected(IndexMetadata indexMetadata, Index index,
                                    Set<String> validNodeIds, DiscoveryNodes.Builder nodes) throws IOException {
        DiscoveryNodes discoveryNodes = nodes.build();
        IndexRoutingTable.Builder indexRoutingTable = createRoutingTable(indexMetadata, index, discoveryNodes);
        assertNodeSelected(indexMetadata, index, validNodeIds, discoveryNodes, indexRoutingTable.build());
    }

    private void assertNodeSelected(IndexMetadata indexMetadata, Index index, Set<String> validNodeIds, DiscoveryNodes nodes,
                                    IndexRoutingTable indexRoutingTable) {
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices.build()))
            .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        List<NodeStats> nodeStats = mockNodeStats(index, nodes, indexRoutingTable, validNodeIds);
        mockNodeStatsCall(nodeStats);

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertSettingsRequestContainsValueFrom(request,
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", validNodeIds, true,
                indexMetadata.getIndex().getName());
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));

        Mockito.verify(client, times(2)).admin();
        Mockito.verify(adminClient, times(1)).cluster();
        Mockito.verify(adminClient, times(1)).indices();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    private void mockNodeStatsCall(List<NodeStats> nodeStatsList) {
        Mockito.doAnswer(invocation -> {
            NodesStatsRequest request = (NodesStatsRequest) invocation.getArguments()[0];
            assertThat(request.requestedMetrics().size(), equalTo(1));
            assertThat(request.indices().isSet(CommonStatsFlags.Flag.Store), equalTo(true));
            @SuppressWarnings("unchecked")
            ActionListener<NodesStatsResponse> listener = (ActionListener<NodesStatsResponse>) invocation.getArguments()[1];
            List<FailedNodeException> failures = new ArrayList<>();
            NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(new ClusterName("cluster-1"), nodeStatsList, failures);
            listener.onResponse(nodesStatsResponse);
            return null;
        }).when(clusterClient).nodesStats(Mockito.any(), Mockito.any());
    }

    private List<NodeStats> mockNodeStats(Index index, DiscoveryNodes nodes, IndexRoutingTable indexRoutingTable,
                                          Set<String> diskAvailableNodeIds) {
        List<NodeStats> nodeStatsList = new ArrayList<>();

        Map<String, List<ShardRouting>> nodeShardRoutings = new HashMap<>();
        for (final IntObjectCursor<IndexShardRoutingTable> shardRoutingTable : indexRoutingTable.getShards()) {
            for (ShardRouting shardRouting : shardRoutingTable.value.shards()) {
                String nodeId = shardRouting.currentNodeId();
                List<ShardRouting> shards;
                if (nodeShardRoutings.containsKey(nodeId)) {
                    shards = nodeShardRoutings.get(nodeId);
                } else {
                    shards = new ArrayList<>();
                }
                shards.add(shardRouting);
                nodeShardRoutings.put(nodeId, shards);
            }
        }

        for (DiscoveryNode discoveryNode : nodes) {
            long nodeFreeBytes = 90;
            long nodeAvailableBytes = 80;
            if (false == diskAvailableNodeIds.contains(discoveryNode.getId())) {
                nodeFreeBytes = 1;
                nodeAvailableBytes = 0;
            }
            FsInfo.Path[] nodeFSInfo = new FsInfo.Path[]{
                new FsInfo.Path("/fs", "/dev/sda", 100, nodeFreeBytes, nodeAvailableBytes)
            };

            Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
            if (nodeShardRoutings.containsKey(discoveryNode.getId())) {
                List<ShardRouting> shards = nodeShardRoutings.get(discoveryNode.getId());
                List<IndexShardStats> indexShardStats = new ArrayList<>();
                for (ShardRouting shardRouting : shards) {
                    Path path = createTempDir().resolve("indices").resolve(shardRouting.shardId().getIndex().getUUID())
                        .resolve(String.valueOf(shardRouting.shardId().id()));
                    ShardPath shardPath = new ShardPath(false, path, path, shardRouting.shardId());
                    CommonStats shardCommonStats = new CommonStats(new CommonStatsFlags(CommonStatsFlags.Flag.Store));
                    shardCommonStats.store.add(new StoreStats(1, -1, -1));
                    ShardStats[] shardStats = new ShardStats[]{new ShardStats(shardRouting, shardPath, shardCommonStats, null, null, null)};
                    indexShardStats.add(new IndexShardStats(shardRouting.shardId(), shardStats));
                }
                statsByShard.put(index, indexShardStats);
            } else {
                statsByShard.put(index, new ArrayList<>());
            }
            NodeIndicesStats nodeIndicesStats = new NodeIndicesStats(new CommonStats(new CommonStatsFlags(CommonStatsFlags.Flag.Store)),
                statsByShard);
            NodeStats nodeStats = new NodeStats(discoveryNode, 0,
                nodeIndicesStats, null, null, null, null, new FsInfo(0, null, nodeFSInfo), null, null, null, null, null, null, null, null);
            nodeStatsList.add(nodeStats);
        }

        return nodeStatsList;
    }

    private void assertNoValidNode(IndexMetadata indexMetadata, Index index, DiscoveryNodes.Builder nodes) {
        DiscoveryNodes discoveryNodes = nodes.build();
        IndexRoutingTable.Builder indexRoutingTable = createRoutingTable(indexMetadata, index, discoveryNodes);

        assertNoValidNode(indexMetadata, index, discoveryNodes, indexRoutingTable.build());
    }

    private void assertNoValidNode(IndexMetadata indexMetadata, Index index, DiscoveryNodes nodes, IndexRoutingTable indexRoutingTable) {

        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices.build()))
            .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        Set<String> diskAvailableNodeIds = new HashSet<>();
        for (DiscoveryNode node : nodes) {
            diskAvailableNodeIds.add(node.getId());
        }
        List<NodeStats> nodeStats = mockNodeStats(index, nodes, indexRoutingTable, diskAvailableNodeIds);
        mockNodeStatsCall(nodeStats);

        expectThrows(NoNodeAvailableException.class,
            () -> PlainActionFuture.<Boolean, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f)));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).cluster();
        Mockito.verify(clusterClient, Mockito.only()).nodesStats(Mockito.any(), Mockito.any());
    }

    private IndexRoutingTable.Builder createRoutingTable(IndexMetadata indexMetadata, Index index, DiscoveryNodes discoveryNodes) {
        assertThat(indexMetadata.getNumberOfReplicas(), lessThanOrEqualTo(discoveryNodes.getSize() - 1));
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : discoveryNodes) {
            nodeIds.add(node.getId());
        }

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        for (int primary = 0; primary < indexMetadata.getNumberOfShards(); primary++) {
            Set<String> nodesThisShardCanBePutOn = new HashSet<>(nodeIds);
            String currentNode = randomFrom(nodesThisShardCanBePutOn);
            nodesThisShardCanBePutOn.remove(currentNode);
            indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(index, primary), currentNode,
                true, ShardRoutingState.STARTED));
            for (int replica = 0; replica < indexMetadata.getNumberOfReplicas(); replica++) {
                assertThat("not enough nodes to allocate all initial shards", nodesThisShardCanBePutOn.size(), greaterThan(0));
                String replicaNode = randomFrom(nodesThisShardCanBePutOn);
                nodesThisShardCanBePutOn.remove(replicaNode);
                indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(index, primary), replicaNode,
                    false, ShardRoutingState.STARTED));
            }
        }
        return indexRoutingTable;
    }

    private IndexRoutingTable.Builder createRoutingTableWithOneShardOnSubset(IndexMetadata indexMetadata, Set<String> subset,
                                                                             Set<String> allNodeIds) {
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        final int numberOfShards = indexMetadata.getNumberOfShards();
        final int shardOnlyOnNewNodes = randomIntBetween(0, numberOfShards - 1);
        for (int primary = 0; primary < indexMetadata.getNumberOfShards(); primary++) {
            Set<String> nodesThisShardCanBePutOn;
            if (primary == shardOnlyOnNewNodes) {
                // This shard should only be allocated to new nodes
                nodesThisShardCanBePutOn = new HashSet<>(subset);
            } else {
                nodesThisShardCanBePutOn = new HashSet<>(allNodeIds);
            }
            String currentNode = randomFrom(nodesThisShardCanBePutOn);
            nodesThisShardCanBePutOn.remove(currentNode);
            indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(indexMetadata.getIndex(), primary), currentNode,
                true, ShardRoutingState.STARTED));
            for (int replica = 0; replica < indexMetadata.getNumberOfReplicas(); replica++) {
                assertThat("not enough nodes to allocate all initial shards", nodesThisShardCanBePutOn.size(), greaterThan(0));
                String replicaNode = randomFrom(nodesThisShardCanBePutOn);
                nodesThisShardCanBePutOn.remove(replicaNode);
                indexRoutingTable.addShard(TestShardRouting.newShardRouting(new ShardId(indexMetadata.getIndex(), primary), replicaNode,
                    false, ShardRoutingState.STARTED));
            }
        }
        return indexRoutingTable;
    }

}
