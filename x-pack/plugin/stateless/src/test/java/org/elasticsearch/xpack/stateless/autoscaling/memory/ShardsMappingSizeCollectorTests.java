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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.CUT_OFF_TIMEOUT_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.RETRY_INITIAL_DELAY_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardsMappingSizeCollectorTests extends ESTestCase {

    private static final boolean IS_INDEX_NODE = true;

    private static final Index TEST_INDEX = new Index("test-index-name-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");

    private static final int FREQUENCY_IN_SECONDS = 1;

    private static final Settings TEST_SETTINGS = Settings.builder()
        .put(CUT_OFF_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(FREQUENCY_IN_SECONDS))
        .put(RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(50))
        .build();

    private final ThreadPool testThreadPool = new TestThreadPool(ShardsMappingSizeCollectorTests.class.getSimpleName());
    private IndicesService indicesService;
    private IndexService indexService;
    private IndexMetadata indexMetadata;
    private ClusterService clusterService;

    @Before
    public void setup() {
        clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(List.<IndexService>of().iterator());

        indexService = mock(IndexService.class);
        indexMetadata = mock(IndexMetadata.class);
        when(indexService.getMetadata()).thenReturn(indexMetadata);
        when(indexMetadata.primaryTerm(0)).thenReturn(0L);
        when(indexMetadata.getIndex()).thenReturn(TEST_INDEX);

        when(indicesService.indexServiceSafe(TEST_INDEX)).thenReturn(indexService);
    }

    @After
    public void cleanup() {
        testThreadPool.shutdownNow();
    }

    public void testPublicationAfterIndexShardStartedWithUnknownVersion() throws Exception {

        ShardId shardId = new ShardId(TEST_INDEX, randomIntBetween(0, 2));
        ShardRouting shardRoutingStub = TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(shardRoutingStub);

        when(indexService.getShardOrNull(shardId.getId())).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes, 1, 1));
        when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);

        var publisher = mock(HeapMemoryUsagePublisher.class);
        var collector = spy(
            new ShardsMappingSizeCollector(IS_INDEX_NODE, indicesService, publisher, testThreadPool, TEST_SETTINGS, clusterService)
        );

        // simulate afterIndexShardStarted
        collector.afterIndexShardStarted(indexShard);
        ArgumentCaptor<HeapMemoryUsage> heapUsageCaptor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        // need to assertBusy here because the publish is done asynchronously
        assertBusy(() -> {
            verify(publisher).publishIndicesMappingSize(heapUsageCaptor.capture(), any());
            assertEquals(ClusterState.UNKNOWN_VERSION, heapUsageCaptor.getValue().clusterStateVersion());
        });
    }

    public void testPublishHeapMemoryUsagesAreRetried() {
        CountDownLatch published = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(randomIntBetween(2, 5));
        var publisher = new HeapMemoryUsagePublisher(new NoOpNodeClient(testThreadPool)) {
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
        var collector = new ShardsMappingSizeCollector(IS_INDEX_NODE, indicesService, publisher, testThreadPool, setting, clusterService);
        var shards = Map.of(new ShardId(TEST_INDEX, 0), new ShardMappingSize(randomNonNegativeInt(), 0, 0, "newTestShardNodeId"));
        var heapUsage = new HeapMemoryUsage(randomNonNegativeLong(), shards);
        collector.publishHeapUsage(heapUsage);
        safeAwait(published);
    }

    public void testIndexMappingRetryRequestAreCancelledAfterTimeout() {
        var unableToPublishMetricsLatch = new CountDownLatch(1);
        var publisher = new HeapMemoryUsagePublisher(new NoOpNodeClient(testThreadPool)) {
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
            testThreadPool,
            TEST_SETTINGS,
            clusterService
        );
        var shards = Map.of(new ShardId(TEST_INDEX, 0), new ShardMappingSize(randomNonNegativeInt(), 0, 0, "newTestShardNodeId"));
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

        safeAwait(unableToPublishMetricsLatch);
    }

    public void testCurrentClusterStateVersionSentWhenSendingUpdatesForShards() throws Exception {
        ShardId shardId = new ShardId(TEST_INDEX, randomIntBetween(0, 2));
        ShardRouting shardRoutingStub = TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(shardRoutingStub);

        when(indexService.getShardOrNull(shardId.id())).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes, 1, 1));
        when(indicesService.indexService(shardId.getIndex())).thenReturn(indexService);

        long currentClusterStateVersion = randomNonNegativeLong();
        when(clusterService.state()).thenReturn(new ClusterState(currentClusterStateVersion, randomUUID(), ClusterState.EMPTY_STATE));

        var publisher = mock(HeapMemoryUsagePublisher.class);
        var collector = new ShardsMappingSizeCollector(
            IS_INDEX_NODE,
            indicesService,
            publisher,
            testThreadPool,
            TEST_SETTINGS,
            clusterService
        );

        collector.updateMappingMetricsForShard(shardId);
        ArgumentCaptor<HeapMemoryUsage> heapUsageCaptor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        // need to assertBusy here because the publish is done asynchronously
        assertBusy(() -> {
            verify(publisher).publishIndicesMappingSize(heapUsageCaptor.capture(), any());
            assertEquals(currentClusterStateVersion, heapUsageCaptor.getValue().clusterStateVersion());
        });
    }
}
