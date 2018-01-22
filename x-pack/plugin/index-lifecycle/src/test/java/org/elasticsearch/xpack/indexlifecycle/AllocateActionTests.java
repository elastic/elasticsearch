/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction.Listener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.instanceOf;

public class AllocateActionTests extends AbstractSerializingTestCase<AllocateAction> {

    @Override
    protected AllocateAction doParseInstance(XContentParser parser) throws IOException {
        return AllocateAction.parse(parser);
    }

    @Override
    protected AllocateAction createTestInstance() {
        Map<String, String> includes = randomMap(0, 100);
        Map<String, String> excludes = randomMap(0, 100);
        Map<String, String> requires = randomMap(0, 100);
        return new AllocateAction(includes, excludes, requires);
    }

    @Override
    protected Reader<AllocateAction> instanceReader() {
        return AllocateAction::new;
    }

    @Override
    protected AllocateAction mutateInstance(AllocateAction instance) throws IOException {
        Map<String, String> include = instance.getInclude();
        Map<String, String> exclude = instance.getExclude();
        Map<String, String> require = instance.getRequire();
        switch (randomIntBetween(0, 2)) {
        case 0:
            include = new HashMap<>(include);
            include.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 1:
            exclude = new HashMap<>(exclude);
            exclude.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 2:
            require = new HashMap<>(require);
            require.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new AllocateAction(include, exclude, require);
    }

    public void testExecuteNoExistingSettings() throws Exception {
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put("index.version.created", Version.CURRENT.id);
        Settings.Builder expectedSettings = Settings.builder();
        includes.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v));
        excludes.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v));
        requires.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v));
        AllocateAction action = new AllocateAction(includes, excludes, requires);

        assertSettingsUpdate(action, existingSettings, expectedSettings.build());
    }

    public void testExecuteSettingsUnassignedShards() throws Exception {
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put("index.version.created", Version.CURRENT.id);
        Settings.Builder expectedSettings = Settings.builder();
        includes.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v));
        excludes.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v));
        requires.forEach((k, v) -> expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v));
        AllocateAction action = new AllocateAction(includes, excludes, requires);

        assertSettingsUpdate(action, existingSettings, expectedSettings.build());
    }

    public void testExecuteSomeExistingSettings() throws Exception {
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put("index.version.created", Version.CURRENT.id);
        Settings.Builder expectedSettings = Settings.builder();
        includes.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            }
        });
        excludes.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            }
        });
        requires.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            }
        });

        // make sure there is at least one setting that is missing
        if (expectedSettings.keys().isEmpty()) {
            String key = randomAlphaOfLengthBetween(1, 20);
            String value = randomAlphaOfLengthBetween(1, 20);
            includes.put(key, value);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value);
        }
        AllocateAction action = new AllocateAction(includes, excludes, requires);

        assertSettingsUpdate(action, existingSettings, expectedSettings.build());
    }

    public void testExecuteSomeExistingSettingsDifferentValue() throws Exception {
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put("index.version.created", Version.CURRENT.id);
        Settings.Builder expectedSettings = Settings.builder();
        includes.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
                existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v + randomAlphaOfLength(4));
            }
        });
        excludes.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
                existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v + randomAlphaOfLength(4));
            }
        });
        requires.forEach((k, v) -> {
            if (randomBoolean()) {
                existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            } else {
                expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
                existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v + randomAlphaOfLength(4));
            }
        });

        // make sure there is at least one setting that is different
        if (expectedSettings.keys().isEmpty()) {
            String key = randomAlphaOfLengthBetween(1, 20);
            String value = randomAlphaOfLengthBetween(1, 20);
            includes.put(key, value);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value);
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value + randomAlphaOfLength(4));
        }
        AllocateAction action = new AllocateAction(includes, excludes, requires);

        assertSettingsUpdate(action, existingSettings, expectedSettings.build());
    }

    public void testExecuteUpdateSettingsFail() throws Exception {
        Settings expectedSettings = Settings.builder().put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "box_type", "foo")
                .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "box_type", "bar")
                .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "box_type", "baz").build();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20))
                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .routingTable(RoutingTable.builder()
                        .add(IndexRoutingTable.builder(index).addShard(
                                TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED)))
                        .build())
                .build();
        Exception exception = new RuntimeException();

        BiConsumer<Settings, Listener> settingsUpdater = (s, l) -> {
            assertEquals(expectedSettings, s);
            l.onFailure(exception);
        };

        Map<String, String> includes = new HashMap<>();
        includes.put("box_type", "foo");
        Map<String, String> excludes = new HashMap<>();
        excludes.put("box_type", "bar");
        Map<String, String> requires = new HashMap<>();
        requires.put("box_type", "baz");

        AllocateAction action = new AllocateAction(includes, excludes, requires);

        RuntimeException thrownException = expectActionFailure(index, clusterState, null, action, settingsUpdater, RuntimeException.class);
        assertSame(exception, thrownException);

    }

    public void testExecuteAllocateComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED));

        AllocateAction action = new AllocateAction(includes, excludes, requires);
        assertAllocateStatus(index, 1, 0, action, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                true);
    }

    public void testExecuteAllocateNotComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), "node2", true, ShardRoutingState.STARTED));

        AllocateAction action = new AllocateAction(includes, excludes, requires);
        assertAllocateStatus(index, 2, 0, action, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                false);
    }

    public void testExecuteAllocateUnassigned() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Map<String, String> includes = randomMap(1, 5);
        Map<String, String> excludes = randomMap(1, 5);
        Map<String, String> requires = randomMap(1, 5);
        Settings.Builder existingSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id)
                .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
        Settings.Builder expectedSettings = Settings.builder();
        Settings.Builder node1Settings = Settings.builder();
        Settings.Builder node2Settings = Settings.builder();
        includes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });
        excludes.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + k, v);
        });
        requires.forEach((k, v) -> {
            existingSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            expectedSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + k, v);
            node1Settings.put(Node.NODE_ATTRIBUTES.getKey() + k, v);
        });

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED))
                .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), null, null, true, ShardRoutingState.UNASSIGNED,
                        new UnassignedInfo(randomFrom(Reason.values()), "the shard is intentionally unassigned")));

        AllocateAction action = new AllocateAction(includes, excludes, requires);
        assertAllocateStatus(index, 2, 0, action, existingSettings, node1Settings, node2Settings, indexRoutingTable,
                false);
    }

    public void testExecuteIndexMissing() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        BiConsumer<Settings, Listener> settingsUpdater = (s, l) -> {
            throw new AssertionError("Unexpected settings update");
        };

        Map<String, String> includes = new HashMap<>();
        includes.put("box_type", "foo");
        Map<String, String> excludes = new HashMap<>();
        excludes.put("box_type", "bar");
        Map<String, String> requires = new HashMap<>();
        requires.put("box_type", "baz");

        AllocateAction action = new AllocateAction(includes, excludes, requires);

        IndexNotFoundException thrownException = expectActionFailure(index, clusterState, null, action, settingsUpdater,
                IndexNotFoundException.class);
        assertEquals("Index not found when executing " + AllocateAction.NAME + " lifecycle action.", thrownException.getMessage());
        assertEquals(index.getName(), thrownException.getIndex().getName());
    }

    private void assertSettingsUpdate(AllocateAction action, Settings.Builder existingSettings, Settings expectedSettings) {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLengthBetween(1, 20)).settings(existingSettings)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Index index = indexMetadata.getIndex();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .routingTable(RoutingTable.builder()
                        .add(IndexRoutingTable.builder(index).addShard(
                                TestShardRouting.newShardRouting(new ShardId(index, 0), "node1", true, ShardRoutingState.STARTED)))
                        .build())
                .build();

        BiConsumer<Settings, Listener> settingsUpdater = (s, l) -> {
            assertEquals(expectedSettings, s);
            l.onSuccess(false);
        };
        assertActionStatus(index, clusterState, null, action, settingsUpdater, false);
    }

    private void assertAllocateStatus(Index index, int shards, int replicas, AllocateAction action, Settings.Builder existingSettings,
            Settings.Builder node1Settings, Settings.Builder node2Settings, IndexRoutingTable.Builder indexRoutingTable,
            boolean expectComplete) {
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName()).settings(existingSettings).numberOfShards(shards)
                .numberOfReplicas(replicas).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(DiscoveryNode.createLocal(node1Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                                "node1"))
                        .add(DiscoveryNode.createLocal(node2Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                                "node2")))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
                Sets.newHashSet(FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
                        FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
                        FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING));

        BiConsumer<Settings, Listener> settingsUpdater = (s, l) -> {
            throw new AssertionError("Unexpected settings update");
        };

        assertActionStatus(index, clusterState, clusterSettings, action, settingsUpdater, expectComplete);
    }

    private void assertActionStatus(Index index, ClusterState clusterState, ClusterSettings clusterSettings, AllocateAction action,
            BiConsumer<Settings, Listener> settingsUpdater, boolean expectComplete) {

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        action.execute(index, settingsUpdater, clusterState, clusterSettings, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call");
            }
        });

        assertEquals(expectComplete, actionCompleted.get());
    }

    private <E> E expectActionFailure(Index index, ClusterState clusterState, ClusterSettings clusterSettings, AllocateAction action,
            BiConsumer<Settings, Listener> settingsUpdater, Class<E> expectedExceptionType) {

        SetOnce<E> exceptionThrown = new SetOnce<>();
        action.execute(index, settingsUpdater, clusterState, clusterSettings, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call");
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(expectedExceptionType));
                exceptionThrown.set((E) e);
            }
        });

        return exceptionThrown.get();
    }

    private Map<String, String> randomMap(int minEntries, int maxEntries) {
        Map<String, String> map = new HashMap<>();
        int numIncludes = randomIntBetween(minEntries, maxEntries);
        for (int i = 0; i < numIncludes; i++) {
            map.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
        return map;
    }

}
