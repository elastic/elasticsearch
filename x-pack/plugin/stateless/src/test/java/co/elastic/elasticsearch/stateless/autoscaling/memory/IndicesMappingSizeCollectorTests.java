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

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.IndicesMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING;
import static org.hamcrest.Matchers.equalTo;
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
            collector.getIndexToMappingSizeMetrics().get(TEST_INDEX),
            equalTo(new IndexMappingSize(testIndexMappingSizeInBytes, "node-0"))
        );

        // invoke publication
        ArgumentCaptor<HeapMemoryUsage> captor = ArgumentCaptor.forClass(HeapMemoryUsage.class);
        collector.publishIndicesMappingSize();
        verify(publisher, times(1)).publishIndicesMappingSize(captor.capture(), any());
        HeapMemoryUsage memoryMetrics = captor.getValue();
        assertThat(memoryMetrics.publicationSeqNo(), equalTo(1L));
        assertThat(
            memoryMetrics.indicesMappingSize().get(TEST_INDEX),
            equalTo(new IndexMappingSize(testIndexMappingSizeInBytes, "node-0"))
        );
    }

    public void testProtectionAgainstFrequentPublication() {

        final List<HeapMemoryUsage> metricsValues = new ArrayList<>();
        final List<ActionListener<ActionResponse.Empty>> nodeClientResponseListeners = new ArrayList<>();
        publisher = new IndicesMappingSizePublisher(new NoOpNodeClient(testThreadPool), () -> TransportVersions.V_8_500_050) {
            @Override
            public void publishIndicesMappingSize(HeapMemoryUsage heapMemoryUsage, ActionListener<ActionResponse.Empty> listener) {
                // collect listeners which are passed to node client
                nodeClientResponseListeners.add(listener);
                metricsValues.add(heapMemoryUsage);
            }
        };
        collector = new IndicesMappingSizeCollector(IS_INDEX_NODE, indicesService, publisher, testThreadPool, TEST_SETTINGS);

        // simulate sequential calls which are made by PublishTask
        int N = randomIntBetween(2, 10);
        for (int i = 0; i < N; i++) {
            // prepare test data in backing map
            // every sending attempt has unique `newTestSizeInBytes` value, so that later it is possible to assert that only first
            // message was sent and the rest were ignored by the publication gate
            final long testSizeInBytes = 1024L + i;
            final String testShardNodeId = "newTestShardNodeId-" + i;
            collector.getIndexToMappingSizeMetrics().put(TEST_INDEX, new IndexMappingSize(testSizeInBytes, testShardNodeId));
            collector.publishIndicesMappingSize();
        }

        // simulate slow call by deferring invoke `onResponse` or `onFailure` on node client listener
        // hence only first call should be able to acquire publication ticket, the rest are ignored
        assertThat(nodeClientResponseListeners.size(), equalTo(1));
        assertThat(metricsValues.size(), equalTo(1));
        assertThat(metricsValues.get(0).indicesMappingSize().get(TEST_INDEX).sizeInBytes(), equalTo(1024L));
        assertThat(metricsValues.get(0).indicesMappingSize().get(TEST_INDEX).metricShardNodeId(), equalTo("newTestShardNodeId-0"));

        // invoke `onResponse` or `onFailure` to clear publication ticket and open the gate
        final ActionListener<ActionResponse.Empty> nodeClientCallListener = nodeClientResponseListeners.get(0);
        if (randomBoolean()) {
            nodeClientCallListener.onResponse(null);
        } else {
            nodeClientCallListener.onFailure(new Exception("TEST Exception is thrown during node client call"));
        }

        // clear registered listener (and metrics) as it is completed by now (purely for test housekeeping)
        nodeClientResponseListeners.clear();
        metricsValues.clear();

        // assert that consequent call succeeds and can acquire publication ticket
        final long newTestSizeInBytes = 1234;
        final String newTestShardNodeId = "newTestShardNodeId-1234";
        collector.getIndexToMappingSizeMetrics().put(TEST_INDEX, new IndexMappingSize(newTestSizeInBytes, newTestShardNodeId));
        collector.publishIndicesMappingSize();

        assertThat(nodeClientResponseListeners.size(), equalTo(1));
        assertThat(metricsValues.size(), equalTo(1));
        assertThat(metricsValues.get(0).indicesMappingSize().get(TEST_INDEX).sizeInBytes(), equalTo(1234L));
        assertThat(metricsValues.get(0).indicesMappingSize().get(TEST_INDEX).metricShardNodeId(), equalTo("newTestShardNodeId-1234"));
    }

    public void testMetricsPublicationOnMasterFailover() throws Exception {

        // test scenario: there is ongoing publication, then master failover is happening and collector immediately pushes metrics to
        // new master, therefore 2 messages are expected to be published
        final CountDownLatch latch = new CountDownLatch(2);

        final List<ActionListener<ActionResponse.Empty>> nodeClientResponseListeners = new ArrayList<>();
        publisher = new IndicesMappingSizePublisher(new NoOpNodeClient(testThreadPool), () -> TransportVersions.V_8_500_050) {
            @Override
            public void publishIndicesMappingSize(HeapMemoryUsage heapMemoryUsage, ActionListener<ActionResponse.Empty> listener) {
                // collect listeners which are passed to node client
                nodeClientResponseListeners.add(listener);
                latch.countDown();
            }
        };
        collector = new IndicesMappingSizeCollector(IS_INDEX_NODE, indicesService, publisher, testThreadPool, TEST_SETTINGS);

        // prepare test data in backing map
        collector.getIndexToMappingSizeMetrics().put(TEST_INDEX, new IndexMappingSize(1024, "node-0"));

        // simulate sequential calls which are made by PublishTask
        int N = randomIntBetween(2, 10);
        for (int i = 0; i < N; i++) {
            collector.publishIndicesMappingSize();
        }

        // simulate slow call by deferring invoke `onResponse` or `onFailure` on node client listener
        // hence only first call should be able to acquire publication ticket, the rest are ignored
        assertThat(nodeClientResponseListeners.size(), equalTo(1));
        assertThat(latch.getCount(), equalTo(1L));   // first message passed the gate

        final boolean firstCallCompletedByNow = randomBoolean();
        if (firstCallCompletedByNow) {
            // completed with either result, publication gate is cleared regardless the call response
            if (randomBoolean()) {
                nodeClientResponseListeners.get(0).onResponse(null);
            } else {
                nodeClientResponseListeners.get(0).onFailure(new Exception("TEST Exception is thrown during node client call"));
            }
            // housekeeping, clean up used listeners
            nodeClientResponseListeners.clear();
        }

        // simulate masterNodeChanged() event to invoke immediate metric publication to new master
        DiscoveryNodes.Delta nodeDelta = mock(DiscoveryNodes.Delta.class);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.nodesDelta()).thenReturn(nodeDelta);
        when(nodeDelta.masterNodeChanged()).thenReturn(true);
        collector.clusterChanged(event);

        latch.await(10, TimeUnit.SECONDS);
        assertThat("Message to new master was not sent", latch.getCount(), equalTo(0L));

        // assert that immediate publication was able to pass through the gate and send metrics over the node client
        if (firstCallCompletedByNow) {
            // by time when masterNodeChanged() event might cause metrics immediate publishing
            // first call to the prev master could finish
            assertThat(nodeClientResponseListeners.size(), equalTo(1));
        } else {
            // assert that there are two simultaneous calls (and hence registered listeners)
            // first (call is still ongoing) is for prev master, second - for current
            assertThat(nodeClientResponseListeners.size(), equalTo(2));
        }

    }
}
