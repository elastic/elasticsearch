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

import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsReader;

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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardSizesCollectorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ShardSizeStatsReader statsReader;
    private ShardSizesPublisher publisher;

    private ShardSizesCollector service;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(ShardSizesCollectorTests.class.getName());
        var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
                .put(ShardSizesCollector.PUSH_DELTA_THRESHOLD_SETTING.getKey(), ByteSizeValue.ofKb(10))
                .build(),
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ShardSizesCollector.PUSH_INTERVAL_SETTING,
                ShardSizesCollector.PUSH_DELTA_THRESHOLD_SETTING
            )
        );

        statsReader = mock(ShardSizeStatsReader.class);
        publisher = mock(ShardSizesPublisher.class);

        service = new ShardSizesCollector(clusterSettings, threadPool, statsReader, publisher, true);
        service.setNodeId("search_node_1");
        service.setMinTransportVersion(TransportVersion.V_8_500_027);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testPushMetrics() throws Exception {

        var shardId = new ShardId("index-1", "_na_", 0);
        var size = new ShardSize(1024, 1024);

        when(statsReader.getShardSize(shardId)).thenReturn(size);
        service.detectShardSize(shardId);
        service.doStart();

        assertBusy(() -> verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId, size)), any()));
    }

    public void testGroupPublications() throws Exception {

        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(1024, 1024);

        when(statsReader.getShardSize(shardId1)).thenReturn(size);
        when(statsReader.getShardSize(shardId2)).thenReturn(size);
        service.detectShardSize(shardId1);
        service.detectShardSize(shardId2);
        service.doStart();

        assertBusy(
            () -> verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any())
        );
    }

    public void testPublishPeriodicallyEvenIfNoChanges() throws Exception {

        var shardId = new ShardId("index-1", "_na_", 0);
        var size = new ShardSize(1024, 1024);

        when(statsReader.getShardSize(shardId)).thenReturn(size);
        service.detectShardSize(shardId);
        service.doStart();

        assertBusy(() -> {
            verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId, size)), any());
            // second empty publication after interval passed
            verify(publisher, atLeastOnce()).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of()), any());
        });
    }

    public void testPublicationIsRetried() throws Exception {

        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(1024, 1024);

        // all initial publications of single shard should fail
        doAnswer(invocation -> {
            invocation.getArgument(2, ActionListener.class).onFailure(new RuntimeException("simulated"));
            return null;
        }).when(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size)), any());

        service.doStart();
        when(statsReader.getShardSize(shardId1)).thenReturn(size);
        service.detectShardSize(shardId1);
        assertBusy(() -> verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size)), any()));

        when(statsReader.getShardSize(shardId2)).thenReturn(size);
        service.detectShardSize(shardId2);
        assertBusy(
            () -> verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any())
        );
    }

    public void testPublishImmediatelyIfBigChangeIsDetected() {

        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var smallSize = new ShardSize(1024, 1024);
        var bigSize = new ShardSize(1024 * 1024, 1024);

        // scheduling is disabled to test immediate sending
        when(statsReader.getShardSize(shardId1)).thenReturn(smallSize);
        service.detectShardSize(shardId1);
        verify(publisher, never()).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());

        when(statsReader.getShardSize(shardId2)).thenReturn(bigSize);
        service.detectShardSize(shardId2);
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, smallSize, shardId2, bigSize)), any());
    }

    public void testPublishAllDataIfMasterChanged() throws Exception {

        var shardId1 = new ShardId("index-1", "_na_", 0);
        var shardId2 = new ShardId("index-2", "_na_", 0);
        var size = new ShardSize(1024, 1024);

        // scheduling is disabled to test immediate sending
        when(statsReader.getAllShardSizes()).thenReturn(Map.of(shardId1, size, shardId2, size));

        var state1 = ClusterState.EMPTY_STATE;
        var state2 = createClusterStateWithNodes(2).build();
        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        assertBusy(
            () -> verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), eq(Map.of(shardId1, size, shardId2, size)), any())
        );
    }

    public void testDeletedIndicesAreRemovedFromState() throws Exception {

        var indexMetadata1 = createIndex(1, 2);
        var indexMetadata2 = createIndex(1, 2);

        var state1 = createClusterStateWithNodes(1).metadata(
            Metadata.builder().put(indexMetadata1, false).put(indexMetadata2, false).build()
        ).build();
        var state2 = ClusterState.builder(state1).metadata(Metadata.builder().put(indexMetadata1, false).build()).build();

        var size = new ShardSize(1024, 1024);
        when(statsReader.getAllShardSizes()).thenReturn(
            Map.ofEntries(
                Map.entry(new ShardId(indexMetadata1.getIndex(), 0), size),
                Map.entry(new ShardId(indexMetadata1.getIndex(), 1), size),
                Map.entry(new ShardId(indexMetadata2.getIndex(), 0), size),
                Map.entry(new ShardId(indexMetadata2.getIndex(), 1), size)
            )
        );
        var listenerWasCalled = new CountDownLatch(1);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<Void>) invocation.getArgument(2, ActionListener.class);
            listener.onResponse(null);
            listenerWasCalled.countDown();
            return null;
        }).when(publisher).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());

        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        assertThat(listenerWasCalled.await(10, TimeUnit.SECONDS), equalTo(true));
        verify(publisher).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());
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

    public void testMovedShardsAreRemovedFromState() throws Exception {

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

        var size = new ShardSize(1024, 1024);
        when(statsReader.getAllShardSizes()).thenReturn(
            Map.of(new ShardId(indexMetadata1.getIndex(), 0), size, new ShardId(indexMetadata2.getIndex(), 0), size)
        );
        var listenerWasCalled = new CountDownLatch(1);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<Void>) invocation.getArgument(2, ActionListener.class);
            listener.onResponse(null);
            listenerWasCalled.countDown();
            return null;
        }).when(publisher).publishSearchShardDiskUsage(eq("search_node_1"), any(), any());

        service.clusterChanged(new ClusterChangedEvent("test", state0, ClusterState.EMPTY_STATE));

        assertThat(listenerWasCalled.await(10, TimeUnit.SECONDS), equalTo(true));
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
        transportVersions.put("master", TransportVersion.V_8_500_027);
        builder.add(DiscoveryNodeUtils.builder("index_node_1").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        transportVersions.put("index_node_1", TransportVersion.V_8_500_027);
        for (int i = 1; i <= searchNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("search_node_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
            transportVersions.put("search_node_" + i, TransportVersion.V_8_500_027);
        }
        builder.masterNodeId("master").localNodeId("search_node_1");
        return ClusterState.builder(ClusterState.EMPTY_STATE).nodes(builder).transportVersions(transportVersions);
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
}
