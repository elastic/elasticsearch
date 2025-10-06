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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.CUT_OFF_TIMEOUT_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.FIXED_HOLLOW_SHARD_MEMORY_OVERHEAD_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.HOLLOW_SHARD_SEGMENT_MEMORY_OVERHEAD_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.RETRY_INITIAL_DELAY_SETTING;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ShardsMappingSizeCollectorTests extends ESTestCase {

    private static final boolean IS_INDEX_NODE = true;

    private static final Index TEST_INDEX = new Index("test-index-name-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");

    private static final int FREQUENCY_IN_SECONDS = 1;

    private static final Settings TEST_SETTINGS = Settings.builder()
        .put(CUT_OFF_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(FREQUENCY_IN_SECONDS))
        .put(RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(50))
        .build();

    private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
    private IndicesService indicesService;
    private IndexService indexService;
    private IndexMetadata indexMetadata;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private HollowShardsService hollowShardsService;

    @Before
    public void setup() {
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            TEST_SETTINGS,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                PUBLISHING_FREQUENCY_SETTING,
                CUT_OFF_TIMEOUT_SETTING,
                RETRY_INITIAL_DELAY_SETTING,
                FIXED_HOLLOW_SHARD_MEMORY_OVERHEAD_SETTING,
                HOLLOW_SHARD_SEGMENT_MEMORY_OVERHEAD_SETTING
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(List.<IndexService>of().iterator());

        indexService = mock(IndexService.class);
        indexMetadata = mock(IndexMetadata.class);
        when(indexService.getMetadata()).thenReturn(indexMetadata);
        when(indexMetadata.primaryTerm(0)).thenReturn(0L);
        when(indexMetadata.getIndex()).thenReturn(TEST_INDEX);

        when(indicesService.indexServiceSafe(TEST_INDEX)).thenReturn(indexService);

        hollowShardsService = mock(HollowShardsService.class);
        when(hollowShardsService.isHollowShard(any())).thenReturn(randomBoolean());
    }

    public void testPublicationAfterIndexShardStartedIncludesClusterStateVersion() {
        ShardId shardId = new ShardId(TEST_INDEX, randomIntBetween(0, 2));
        ShardRouting shardRoutingStub = TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(shardRoutingStub);
        when(indexShard.getShardFieldStats()).thenReturn(
            new ShardFieldStats(
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
        );

        when(indexService.getShardOrNull(shardId.getId())).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes, 1, 1));
        when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);

        var publisher = mock(HeapMemoryUsagePublisher.class);
        var collector = spy(
            new ShardsMappingSizeCollector(
                IS_INDEX_NODE,
                indicesService,
                publisher,
                deterministicTaskQueue.getThreadPool(),
                clusterService,
                hollowShardsService
            )
        );

        final var firstState = ClusterStateCreationUtils.state(randomIntBetween(1, 3), new String[] { TEST_INDEX.getName() }, 3);
        final var secondState = ClusterState.builder(firstState).incrementVersion().build();

        // simulate afterIndexShardStarted, clusterChanged
        collector.afterIndexShardStarted(indexShard);
        collector.clusterChanged(new ClusterChangedEvent("test", secondState, firstState));
        deterministicTaskQueue.runAllTasks();
        ArgumentCaptor<HeapMemoryUsage> heapUsageCaptor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        verify(publisher).publishIndicesMappingSize(heapUsageCaptor.capture(), any());
        HeapMemoryUsage publishedUsage = heapUsageCaptor.getValue();
        assertEquals(secondState.version(), publishedUsage.clusterStateVersion());
        assertThat(publishedUsage.shardMappingSizes(), hasKey(shardId));

        // Verify shard is not published again next update
        final var thirdState = ClusterState.builder(secondState).incrementVersion().build();
        collector.clusterChanged(new ClusterChangedEvent("test", thirdState, secondState));
        deterministicTaskQueue.runAllTasks();
        verifyNoMoreInteractions(publisher);
    }

    public void testPublishHeapMemoryUsagesAreRetried() {
        CountDownLatch published = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(randomIntBetween(2, 5));
        var publisher = new HeapMemoryUsagePublisher(new NoOpNodeClient(deterministicTaskQueue.getThreadPool())) {
            @Override
            public void publishIndicesMappingSize(HeapMemoryUsage heapMemoryUsage, ActionListener<ActionResponse.Empty> listener) {
                if (attempts.decrementAndGet() == 0) {
                    listener.onResponse(ActionResponse.Empty.INSTANCE);
                    published.countDown();
                } else {
                    listener.onFailure(
                        new RemoteTransportException(
                            "Memory metrics service error",
                            new AutoscalingMissedIndicesUpdateException("Unable to publish metrics")
                        )
                    );
                }
            }
        };
        // Use a higher CUT_OFF_TIMEOUT since this serves as the retry timeout
        var setting = Settings.builder()
            .put(CUT_OFF_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .put(RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(50))
            .build();
        clusterSettings.applySettings(setting);
        var collector = new ShardsMappingSizeCollector(
            IS_INDEX_NODE,
            indicesService,
            publisher,
            deterministicTaskQueue.getThreadPool(),
            clusterService,
            hollowShardsService
        );
        var shards = Map.of(
            new ShardId(TEST_INDEX, 0),
            new ShardMappingSize(randomNonNegativeInt(), 0, 0, 0L, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, "newTestShardNodeId")
        );
        var heapUsage = new HeapMemoryUsage(randomNonNegativeLong(), shards);
        collector.publishHeapUsage(heapUsage);

        deterministicTaskQueue.runAllTasks();
        safeAwait(published);
    }

    public void testIndexMappingRetryRequestAreCancelledAfterTimeout() {
        var unableToPublishMetricsLatch = new CountDownLatch(1);
        var publisher = new HeapMemoryUsagePublisher(new NoOpNodeClient(deterministicTaskQueue.getThreadPool())) {
            @Override
            public void publishIndicesMappingSize(HeapMemoryUsage heapMemoryUsage, ActionListener<ActionResponse.Empty> listener) {
                logger.info("Publishing {}", heapMemoryUsage);
                listener.onFailure(
                    new RemoteTransportException(
                        "Memory metrics service error",
                        new AutoscalingMissedIndicesUpdateException("Unable to publish metrics")
                    )
                );
            }
        };
        var collector = new ShardsMappingSizeCollector(
            IS_INDEX_NODE,
            indicesService,
            publisher,
            deterministicTaskQueue.getThreadPool(),
            clusterService,
            hollowShardsService
        );
        var shards = Map.of(
            new ShardId(TEST_INDEX, 0),
            new ShardMappingSize(randomNonNegativeInt(), 0, 0, 0L, 0L, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, "newTestShardNodeId")
        );
        var heapUsage = new HeapMemoryUsage(randomNonNegativeLong(), shards);
        collector.publishHeapUsage(heapUsage, TimeValue.timeValueMillis(500), new ActionListener<ActionResponse.Empty>() {
            @Override
            public void onResponse(ActionResponse.Empty empty) {}

            @Override
            public void onFailure(Exception e) {
                // onFailure gets called with the latest thrown exception
                var cause = e.getCause();
                assertEquals(AutoscalingMissedIndicesUpdateException.class, cause.getClass());
                assertEquals("Unable to publish metrics", cause.getMessage());
                unableToPublishMetricsLatch.countDown();
            }
        });

        deterministicTaskQueue.runAllTasks();
        safeAwait(unableToPublishMetricsLatch);
    }

    public void testCurrentClusterStateVersionSentWhenSendingUpdatesForShards() {
        ShardId shardId = new ShardId(TEST_INDEX, randomIntBetween(0, 2));
        ShardRouting shardRoutingStub = TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(shardRoutingStub);
        when(indexShard.getShardFieldStats()).thenReturn(new ShardFieldStats(1, 1, -1, 0, 0));

        when(indexService.getShardOrNull(shardId.id())).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes, 1, 1));
        when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);

        when(hollowShardsService.isHollowShard(any())).thenReturn(false);
        final var shards = Map.of(
            shardId,
            new ShardMappingSize(testIndexMappingSizeInBytes, 1, 1, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, "node-0")
        );

        long currentClusterStateVersion = randomNonNegativeLong();
        when(clusterService.state()).thenReturn(new ClusterState(currentClusterStateVersion, randomUUID(), ClusterState.EMPTY_STATE));

        var publisher = mock(HeapMemoryUsagePublisher.class);
        var collector = new ShardsMappingSizeCollector(
            IS_INDEX_NODE,
            indicesService,
            publisher,
            deterministicTaskQueue.getThreadPool(),
            clusterService,
            hollowShardsService
        );

        collector.updateMappingMetricsForShard(shardId);

        deterministicTaskQueue.runAllTasks();
        ArgumentCaptor<HeapMemoryUsage> heapUsageCaptor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        verify(publisher).publishIndicesMappingSize(heapUsageCaptor.capture(), any());
        assertEquals(currentClusterStateVersion, heapUsageCaptor.getValue().clusterStateVersion());
        assertEquals(shards, heapUsageCaptor.getValue().shardMappingSizes());
    }

    public void testHollowShardMemoryOverheadCalculation() throws Exception {
        ShardId shardId = new ShardId(TEST_INDEX, randomIntBetween(0, 2));

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED));
        final int numSegments = randomIntBetween(1, 10);
        when(indexShard.getShardFieldStats()).thenReturn(new ShardFieldStats(numSegments, 1, -1, 10, 20));

        when(indexService.getShardOrNull(shardId.id())).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes, 1, 1));
        when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);

        var publisher = mock(HeapMemoryUsagePublisher.class);
        var collector = new ShardsMappingSizeCollector(
            IS_INDEX_NODE,
            indicesService,
            publisher,
            deterministicTaskQueue.getThreadPool(),
            clusterService,
            hollowShardsService
        );

        long fixedHollowShardMemoryOverheadBytes = randomLongBetween(1, 10_000_000);
        long hollowShardSegmentMemoryOverheadBytes = randomLongBetween(1, 10_000_000);
        clusterSettings.applySettings(
            Settings.builder()
                .put(FIXED_HOLLOW_SHARD_MEMORY_OVERHEAD_SETTING.getKey(), ByteSizeValue.ofBytes(fixedHollowShardMemoryOverheadBytes))
                .put(HOLLOW_SHARD_SEGMENT_MEMORY_OVERHEAD_SETTING.getKey(), ByteSizeValue.ofBytes(hollowShardSegmentMemoryOverheadBytes))
                .build()
        );
        when(hollowShardsService.isHollowShard(any())).thenReturn(true);
        long shardMemoryOverhead = hollowShardSegmentMemoryOverheadBytes * numSegments + fixedHollowShardMemoryOverheadBytes;
        final var shards = Map.of(
            shardId,
            new ShardMappingSize(testIndexMappingSizeInBytes, numSegments, 1, 10, 20, shardMemoryOverhead, "node-0")
        );

        collector.updateMappingMetricsForShard(shardId, randomNonNegativeLong());
        deterministicTaskQueue.runAllTasks();
        ArgumentCaptor<HeapMemoryUsage> heapUsageCaptor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        verify(publisher).publishIndicesMappingSize(heapUsageCaptor.capture(), any());
        assertEquals(shards, heapUsageCaptor.getValue().shardMappingSizes());
    }

}
