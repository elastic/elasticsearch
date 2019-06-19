/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep.Listener;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class SetSingleNodeAllocateStepTests extends AbstractStepTestCase<SetSingleNodeAllocateStep> {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

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
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Settings nodeSettings = Settings.builder().put(validNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName).build();
            nodes.add(
                    DiscoveryNode.createLocal(nodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, nodePort), nodeId));
            validNodeIds.add(nodeId);
        }

        assertNodeSelected(indexMetaData, index, validNodeIds, nodes);
    }

    public void testPerformActionAttrsAllNodesValid() throws IOException {
        int numAttrs = randomIntBetween(1, 10);
        String[][] validAttrs = new String[numAttrs][2];
        for (int i = 0; i < numAttrs; i++) {
            validAttrs[i] = new String[] { randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20) };
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        for (String[] attr : validAttrs) {
            indexSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attr[0], attr[1]);
        }
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
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

        assertNodeSelected(indexMetaData, index, validNodeIds, nodes);
    }

    public void testPerformActionAttrsSomeNodesValid() throws IOException {
        String[] validAttr = new String[] { "box_type", "valid" };
        String[] invalidAttr = new String[] { "box_type", "not_valid" };
        Settings.Builder indexSettings = settings(Version.CURRENT);
        indexSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + validAttr[0], validAttr[1]);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + validAttr[0], validAttr[1]).build();
        Settings invalidNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + invalidAttr[0], invalidAttr[1]).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
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

        assertNodeSelected(indexMetaData, index, validNodeIds, nodes);
    }

    public void testPerformActionAttrsNoNodesValid() {
        String[] validAttr = new String[] { "box_type", "valid" };
        String[] invalidAttr = new String[] { "box_type", "not_valid" };
        Settings.Builder indexSettings = settings(Version.CURRENT);
        indexSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + validAttr[0], validAttr[1]);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Settings invalidNodeSettings = Settings.builder().put(Node.NODE_ATTRIBUTES.getKey() + invalidAttr[0], invalidAttr[1]).build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_id_" + i;
            String nodeName = "node_" + i;
            int nodePort = 9300 + i;
            Builder nodeSettingsBuilder = Settings.builder().put(invalidNodeSettings).put(Node.NODE_NAME_SETTING.getKey(), nodeName);
            nodes.add(DiscoveryNode.createLocal(nodeSettingsBuilder.build(), new TransportAddress(TransportAddress.META_ADDRESS, nodePort),
                    nodeId));
        }

        assertNoValidNode(indexMetaData, index, nodes);
    }

    public void testPerformActionAttrsRequestFails() {
        int numAttrs = randomIntBetween(1, 10);
        Map<String, String> validAttributes = new HashMap<>();
        for (int i = 0; i < numAttrs; i++) {
            validAttributes.put(randomValueOtherThanMany(validAttributes::containsKey,
                () -> randomAlphaOfLengthBetween(1,20)), randomAlphaOfLengthBetween(1,20));
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        validAttributes.forEach((k, v) -> {
            indexSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);

        });
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(indexSettings)
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
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

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetaData);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node_id_0", true, ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();
        Exception exception = new RuntimeException();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
                assertSettingsRequestContainsValueFrom(request,
                    IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", validNodeIds, true,
                    indexMetaData.getIndex().getName());
                listener.onFailure(exception);
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, clusterState, null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                exceptionThrown.set(true);
            }
        });

        assertEquals(true, exceptionThrown.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionAttrsNoShard() {
        int numAttrs = randomIntBetween(1, 10);
        String[][] validAttrs = new String[numAttrs][2];
        for (int i = 0; i < numAttrs; i++) {
            validAttrs[i] = new String[] { randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20) };
        }
        Settings.Builder indexSettings = settings(Version.CURRENT);
        for (String[] attr : validAttrs) {
            indexSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + attr[0], attr[1]);
        }
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(indexSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetaData.getIndex();
        Set<String> validNodeIds = new HashSet<>();
        Settings validNodeSettings = Settings.EMPTY;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        int numNodes = randomIntBetween(1, 20);
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

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetaData);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, clusterState, null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, Matchers.instanceOf(IndexNotFoundException.class));
                assertEquals(indexMetaData.getIndex(), ((IndexNotFoundException) e).getIndex());
                exceptionThrown.set(true);
            }
        });

        assertEquals(true, exceptionThrown.get());

        Mockito.verifyZeroInteractions(client);
    }

    private void assertNodeSelected(IndexMetaData indexMetaData, Index index,
                                    Set<String> validNodeIds, DiscoveryNodes.Builder nodes) throws IOException {
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetaData);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node_id_0", true, ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
                assertSettingsRequestContainsValueFrom(request,
                        IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", validNodeIds, true,
                        indexMetaData.getIndex().getName());
                listener.onResponse(new AcknowledgedResponse(true));
                return null;
            }

        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetaData, clusterState, null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    private void assertNoValidNode(IndexMetaData indexMetaData, Index index, DiscoveryNodes.Builder nodes) {
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetaData);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node_id_0", true, ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(nodes).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        SetSingleNodeAllocateStep step = createRandomInstance();

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetaData, clusterState, null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(false, actionCompleted.get());

        Mockito.verifyZeroInteractions(client);
    }

}
