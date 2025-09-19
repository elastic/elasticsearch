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

import co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector.ShardMergeMemoryEstimate;

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector.AUTOSCALING_MERGE_MEMORY_ESTIMATE_SERVERLESS_VERSION;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector.MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

public class MergeMemoryEstimateCollectorTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void teardown() {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testPublishOnMergeEvent() throws Exception {
        var settings = new ClusterSettings(
            Settings.builder().put(MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO.getKey(), 0.1).build(),
            Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO)
        );
        List<TransportPublishMergeMemoryEstimate.Request> requests = Collections.synchronizedList(new ArrayList<>());
        var collector = new MergeMemoryEstimateCollector(
            settings,
            () -> AUTOSCALING_MERGE_MEMORY_ESTIMATE_SERVERLESS_VERSION,
            () -> "node1",
            requests::add
        );
        {
            var merge1 = new OnGoingMerge(mock(MergePolicy.OneMerge.class));
            collector.onMergeQueued(merge1, 1024);
            assertThat(requests.size(), equalTo(1));
            var request = requests.getLast();
            assertThat(request.getNodeEphemeralId(), equalTo("node1"));
            assertThat(request.getEstimate().estimateInBytes(), equalTo(1024L));
            // a new merge which is not high enough (based on the min_change_ratio) is not published
            var merge2 = new OnGoingMerge(mock(MergePolicy.OneMerge.class));
            collector.onMergeQueued(merge2, (long) (1024 * randomDoubleBetween(1.0, 1.1, true)));
            assertThat(requests.size(), equalTo(1));
            assertThat(request.getNodeEphemeralId(), equalTo("node1"));
            assertThat(request.getEstimate().estimateInBytes(), equalTo(1024L));

            collector.onMergeCompleted(merge2);
            assertThat(requests.size(), equalTo(1));
            collector.onMergeCompleted(merge1);
            assertThat(requests.size(), equalTo(2));
            assertThat(request.getNodeEphemeralId(), equalTo("node1"));
            request = requests.getLast();
            assertThat(request.getEstimate(), equalTo(ShardMergeMemoryEstimate.NO_MERGES));
        }
        {
            requests.clear();
            var merge1 = new OnGoingMerge(mock(MergePolicy.OneMerge.class));
            var merge1Estimate = new ShardMergeMemoryEstimate(merge1.getId(), 1024);
            var merge2 = new OnGoingMerge(mock(MergePolicy.OneMerge.class));
            var merge2Estimate = new ShardMergeMemoryEstimate(merge2.getId(), 2048);

            collector.onMergeQueued(merge1, merge1Estimate.estimateInBytes());
            assertThat(requests.size(), equalTo(1));
            assertThat(requests.getLast().getEstimate(), equalTo(merge1Estimate));
            assertThat(collector.getCurrentNodeEstimate(), equalTo(merge1Estimate));

            collector.onMergeQueued(merge2, merge2Estimate.estimateInBytes());
            assertThat(requests.size(), equalTo(2));
            assertThat(requests.getLast().getEstimate(), equalTo(merge2Estimate));
            assertThat(collector.getCurrentNodeEstimate(), equalTo(merge2Estimate));

            if (randomBoolean()) {
                collector.onMergeAborted(merge2);
            } else {
                collector.onMergeCompleted(merge2);
            }
            assertThat(requests.size(), equalTo(3));
            assertThat(requests.getLast().getEstimate(), equalTo(merge1Estimate));
            assertThat(collector.getCurrentNodeEstimate(), equalTo(merge1Estimate));

            if (randomBoolean()) {
                collector.onMergeAborted(merge1);
            } else {
                collector.onMergeCompleted(merge1);
            }
            assertThat(requests.size(), equalTo(4));
            assertThat(requests.getLast().getEstimate(), equalTo(ShardMergeMemoryEstimate.NO_MERGES));
            assertThat(collector.getCurrentNodeEstimate(), equalTo(ShardMergeMemoryEstimate.NO_MERGES));
        }
        {
            // master node change causes republication
            requests.clear();
            var builder = DiscoveryNodes.builder()
                .add(DiscoveryNodeUtils.create("node0"))
                .add(DiscoveryNodeUtils.create("node1"))
                .localNodeId("node0")
                .masterNodeId("node0");
            final var clusterState1 = ClusterState.builder(new ClusterName("test"))
                .nodes(builder.build())
                .version(randomLongBetween(0, 1000))
                .build();
            collector.clusterChanged(new ClusterChangedEvent("test", clusterState1, ClusterState.EMPTY_STATE));
            assertThat(requests.size(), equalTo(0));
            var merge = new OnGoingMerge(mock(MergePolicy.OneMerge.class));
            collector.onMergeQueued(merge, 1024);
            assertThat(requests.size(), equalTo(1));
            assertThat(requests.getLast().getEstimate().estimateInBytes(), equalTo(1024L));
            requests.clear();
            final var clusterState2 = ClusterState.builder(clusterState1)
                .nodes(builder.masterNodeId("node1").build())
                .version(clusterState1.version() + 1)
                .build();
            collector.clusterChanged(new ClusterChangedEvent("test", clusterState2, clusterState1));
            assertThat(requests.size(), equalTo(1));
            assertThat(requests.getLast().getEstimate().estimateInBytes(), equalTo(1024L));
        }
    }
}
