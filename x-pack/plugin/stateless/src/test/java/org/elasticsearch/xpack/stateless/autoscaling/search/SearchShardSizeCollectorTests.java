/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsClient;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.unit.ByteSizeUnit.KB;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SearchShardSizeCollectorTests extends ESTestCase {

    private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
    private final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
    private ClusterSettings clusterSettings;
    private ShardSizeStatsClient statsClient;
    private ShardSizesPublisher publisher;

    private SearchShardSizeCollector service;

    private final PrimaryTermAndGeneration primaryTermGeneration = new PrimaryTermAndGeneration(
        randomNonNegativeLong(),
        randomNonNegativeLong()
    );

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(SearchShardSizeCollector.PUSH_DELTA_THRESHOLD_SETTING.getKey(), ByteSizeValue.ofBytes(SIZE_THRESHOLD_BYTES))
                .build(),
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ServerlessSharedSettings.BOOST_WINDOW_SETTING,
                SearchShardSizeCollector.PUSH_INTERVAL_SETTING,
                SearchShardSizeCollector.PUSH_DELTA_THRESHOLD_SETTING
            )
        );

        statsClient = mock(ShardSizeStatsClient.class);
        publisher = mock(ShardSizesPublisher.class);
        doAnswer(invocation -> {
            final ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(publisher).publishSearchShardDiskUsage(any(), any(), any());

        service = new SearchShardSizeCollector(clusterSettings, threadPool, statsClient, publisher);
        service.setNodeId("search_node_1");
        service.start();
    }

    @After
    public void drainQueue() {
        service.close();
        deterministicTaskQueue.runAllTasks();
        service = null;
    }

    public void testPushMetrics() {
        var shardId = new ShardId("index-1", "_na_", 0);
        var size = new ShardSize(largeSizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpShardSize(shardId, size);
        service.collectShardSize(shardId);

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId, size)), any());
    }

    public void testGroupPublications() {
        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(largeSizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpShardSize(shardId1, size);
        setUpShardSize(shardId2, size);

        service.collectShardSize(shardId1);
        service.collectShardSize(shardId2);

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any());
    }

    public void testPublishPeriodicallyEvenIfNoChanges() {
        var shardId = new ShardId("index-1", "_na_", 0);
        var size = new ShardSize(largeSizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpShardSize(shardId, size);
        service.collectShardSize(shardId);

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId, size)), any());

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        // second empty publication after interval passed
        verify(publisher, atLeastOnce()).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of()), any());
    }

    public void testEmptyShardSizeIsPublished() {
        var shardId = new ShardId("index-1", "_na_", 0);
        var size = ShardSize.EMPTY;

        setUpShardSize(shardId, size);
        service.collectShardSize(shardId);

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId, size)), any());
    }

    public void testPublicationIsRetried() {
        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(largeSizeBytes(), anySizeBytes(), primaryTermGeneration);

        // all initial publications of single shard should fail
        doAnswer(invocation -> {
            invocation.getArgument(2, ActionListener.class).onFailure(new RuntimeException("simulated"));
            return null;
        }).when(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size)), any());

        setUpShardSize(shardId1, size);
        service.collectShardSize(shardId1);

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size)), any());

        setUpShardSize(shardId2, size);
        service.collectShardSize(shardId2);

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any());
    }

    public void testPublishImmediatelyIfBigChangeIsDetected() {
        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var smallSize = new ShardSize(smallSizeBytes(), anySizeBytes(), primaryTermGeneration);
        var bigSize = new ShardSize(largeSizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpShardSize(shardId1, smallSize);
        setUpShardSize(shardId2, bigSize);

        service.collectShardSize(shardId1);

        // not publishing immediately after small change in shardId1
        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher, never()).publishSearchShardDiskUsage(any(), any(), any());

        service.collectShardSize(shardId2);
        deterministicTaskQueue.runAllRunnableTasks();
        // publishing immediately all once big change detected in shardId2
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, smallSize, shardId2, bigSize)), any());
    }

    public void testPublishAllDataIfMasterChanged() {
        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(anySizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpAllShardSizes(Map.of(shardId1, size, shardId2, size));

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher, never()).publishSearchShardDiskUsage(any(), any(), any());

        var state1 = ClusterState.EMPTY_STATE;
        var state2 = createClusterStateWithNodes(2).build();
        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any());
    }

    public void testPublishAllDataIfInteractiveDataAgeSettingChanges() {
        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(anySizeBytes(), anySizeBytes(), primaryTermGeneration);

        setUpAllShardSizes(Map.of(shardId1, size, shardId2, size));

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher, never()).publishSearchShardDiskUsage(any(), any(), any());

        clusterSettings.applySettings(
            Settings.builder()
                .put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(14))
                .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
                .put(SearchShardSizeCollector.PUSH_DELTA_THRESHOLD_SETTING.getKey(), ByteSizeValue.ofKb(10))
                .build()
        );

        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any());
    }

    public void testDeletedIndicesAreRemovedFromState() {

        var indexMetadata1 = createIndex(1, 2);
        var indexMetadata2 = createIndex(1, 2);

        var state1 = createClusterStateWithNodes(1).metadata(
            Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false).build()
        ).build();
        var state2 = ClusterState.builder(state1).metadata(Metadata.builder().put(indexMetadata1, false).build()).build();

        var size = new ShardSize(anySizeBytes(), anySizeBytes(), primaryTermGeneration);
        setUpAllShardSizes(
            Map.ofEntries(
                Map.entry(new ShardId(indexMetadata1.getIndex(), 0), size),
                Map.entry(new ShardId(indexMetadata1.getIndex(), 1), size),
                Map.entry(new ShardId(indexMetadata2.getIndex(), 0), size),
                Map.entry(new ShardId(indexMetadata2.getIndex(), 1), size)
            )
        );

        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher, atLeastOnce()).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());

        assertThat(
            service.getPastPublications(),
            allOf(
                aMapWithSize(4),
                hasKey(new ShardId(indexMetadata1.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata1.getIndex(), 1)),
                hasKey(new ShardId(indexMetadata2.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata2.getIndex(), 1))
            )
        );

        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertThat(
            service.getPastPublications(),
            allOf(
                aMapWithSize(2),
                hasKey(new ShardId(indexMetadata1.getIndex(), 0)),
                hasKey(new ShardId(indexMetadata1.getIndex(), 1)),
                not(hasKey(new ShardId(indexMetadata2.getIndex(), 0))),
                not(hasKey(new ShardId(indexMetadata2.getIndex(), 1)))
            )
        );
    }

    public void testMovedShardsAreRemovedFromState() {

        var indexMetadata1 = createIndex(1, 1);
        var indexMetadata2 = createIndex(1, 1);

        var state0 = createClusterStateWithNodes(1).metadata(
            Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false).build()
        ).build();
        var state1 = ClusterState.builder(state0)
            .routingTable(
                RoutingTable.builder()
                    .add(createIndexRoutingTable(indexMetadata1.getIndex(), "search_node_1"))
                    .add(createIndexRoutingTable(indexMetadata2.getIndex(), "search_node_1"))
                    .build()
            )
            .build();
        var state2 = ClusterState.builder(state1)
            .routingTable(
                RoutingTable.builder()
                    .add(createIndexRoutingTable(indexMetadata1.getIndex(), "search_node_1"))
                    .add(createIndexRoutingTable(indexMetadata2.getIndex(), "search_node_2"))
                    .build()
            )
            .build();

        var size = new ShardSize(anySizeBytes(), anySizeBytes(), primaryTermGeneration);
        setUpAllShardSizes(Map.of(new ShardId(indexMetadata1.getIndex(), 0), size, new ShardId(indexMetadata2.getIndex(), 0), size));

        service.clusterChanged(new ClusterChangedEvent("test", state0, ClusterState.EMPTY_STATE));
        deterministicTaskQueue.runAllRunnableTasks();
        verify(publisher, atLeastOnce()).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());

        assertThat(
            service.getPastPublications(),
            allOf(aMapWithSize(2), hasKey(new ShardId(indexMetadata1.getIndex(), 0)), hasKey(new ShardId(indexMetadata2.getIndex(), 0)))
        );

        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertThat(
            service.getPastPublications(),
            allOf(
                aMapWithSize(1),
                hasKey(new ShardId(indexMetadata1.getIndex(), 0)),
                not(hasKey(new ShardId(indexMetadata2.getIndex(), 0)))
            )
        );
    }

    private static ClusterState.Builder createClusterStateWithNodes(int searchNodes) {
        var builder = DiscoveryNodes.builder();
        var transportVersions = new HashMap<String, TransportVersion>();
        builder.add(DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build());
        transportVersions.put("master", TransportVersion.current());
        builder.add(DiscoveryNodeUtils.builder("index_node_1").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        transportVersions.put("index_node_1", TransportVersion.current());
        for (int i = 1; i <= searchNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("search_node_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
            transportVersions.put("search_node_" + i, TransportVersion.current());
        }
        builder.masterNodeId("master").localNodeId("search_node_1");
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(builder)
            .nodeIdsToCompatibilityVersions(Maps.transformValues(transportVersions, tv -> new CompatibilityVersions(tv, Map.of())));
    }

    private static IndexMetadata createIndex(int shards, int replicas) {
        return IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(shards, replicas).put("index.version.created", Version.CURRENT))
            .build();
    }

    private static IndexRoutingTable createIndexRoutingTable(Index index, String nodeId) {
        return IndexRoutingTable.builder(index)
            .addShard(newShardRouting(new ShardId(index, 0), nodeId, true, ShardRoutingState.STARTED))
            .build();
    }

    private void setUpShardSize(ShardId shardId, ShardSize size) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<ShardSize>) invocation.getArgument(2, ActionListener.class);
            listener.onResponse(size);
            return null;
        }).when(statsClient).getShardSize(eq(shardId), any(), any());
    }

    private void setUpAllShardSizes(Map<ShardId, ShardSize> sizes) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<Map<ShardId, ShardSize>>) invocation.getArgument(1, ActionListener.class);
            listener.onResponse(sizes);
            return null;
        }).when(statsClient).getAllShardSizes(any(), any());
    }

    private static final long SIZE_THRESHOLD_BYTES = KB.toBytes(10);

    private static long smallSizeBytes() {
        return randomLongBetween(1, SIZE_THRESHOLD_BYTES - 1);
    }

    private static long largeSizeBytes() {
        return randomLongBetween(SIZE_THRESHOLD_BYTES + 1, SIZE_THRESHOLD_BYTES * 2);
    }

    private static long anySizeBytes() {
        return randomLongBetween(1, SIZE_THRESHOLD_BYTES * 2);
    }
}
