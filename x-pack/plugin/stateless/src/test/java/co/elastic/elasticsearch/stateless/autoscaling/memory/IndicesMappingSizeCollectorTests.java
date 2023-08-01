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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.IndicesMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndicesMappingSizeCollectorTests extends ESTestCase {

    private static final boolean IS_INDEX_NODE = true;

    private static final Index TEST_INDEX = new Index("test-index-name-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");

    private static final int FREQUENCY_IN_SECONDS = 1;

    private static final Settings TEST_SETTINGS = Settings.builder()
        .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(FREQUENCY_IN_SECONDS))
        .build();

    private final ThreadPool testThreadPool = new TestThreadPool(IndicesMappingSizeCollectorTests.class.getSimpleName());

    private IndicesService indicesService;

    private IndexService indexService;

    private IndexMetadata indexMetadata;

    private IndicesMappingSizePublisher publisher;

    private IndicesMappingSizeCollector collector;

    @Before
    public void setup() {

        indicesService = mock(IndicesService.class);
        publisher = mock(IndicesMappingSizePublisher.class);
        collector = spy(new IndicesMappingSizeCollector(IS_INDEX_NODE, indicesService, publisher, testThreadPool, TEST_SETTINGS));

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

    public void testMetricPublicationThreadIsRunning() throws Exception {

        // setup to invoke listener
        DiscoveryNodes.Delta nodeDelta = mock(DiscoveryNodes.Delta.class);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.nodesDelta()).thenReturn(nodeDelta);
        when(nodeDelta.masterNodeChanged()).thenReturn(true);

        collector.clusterChanged(event);

        final long asyncDelayInMillis = FREQUENCY_IN_SECONDS * 1000 + randomLongBetween(1000, 2000);
        verify(collector, Mockito.after(asyncDelayInMillis).times(1)).publishIndicesMappingSize();
        verify(publisher, never()).publishIndicesMappingSize(any(), any());
    }

    public void testPublicationAfterIndexShardStarted() {

        ShardId shardId = new ShardId(TEST_INDEX, 0);
        ShardRouting shardRoutingStub = TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(shardRoutingStub);

        when(indexService.getShardOrNull(0)).thenReturn(indexShard);
        final long testIndexMappingSizeInBytes = 1024;
        when(indexService.getNodeMappingStats()).thenReturn(new NodeMappingStats(1, testIndexMappingSizeInBytes));

        // simulate event
        collector.afterIndexShardStarted(indexShard);
        assertThat(
            new IndexMappingSize(testIndexMappingSizeInBytes, "node-0"),
            Matchers.equalTo(collector.getIndexToMappingSizeMetrics().get(TEST_INDEX))
        );

        // invoke publication
        ArgumentCaptor<HeapMemoryUsage> captor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        collector.publishIndicesMappingSize();
        verify(publisher, times(1)).publishIndicesMappingSize(captor.capture(), any());
        HeapMemoryUsage memoryMetrics = captor.getValue();
        assertThat(1L, Matchers.equalTo(memoryMetrics.publicationSeqNo()));
        assertThat(
            new IndexMappingSize(testIndexMappingSizeInBytes, "node-0"),
            Matchers.equalTo(memoryMetrics.indicesMappingSize().get(TEST_INDEX))
        );
    }
}
