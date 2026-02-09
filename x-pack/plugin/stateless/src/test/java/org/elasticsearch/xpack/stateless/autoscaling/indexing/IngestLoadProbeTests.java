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

package org.elasticsearch.xpack.stateless.autoscaling.indexing;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProviderUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestionLoad.ExecutorStats;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.core.TimeValue.ZERO;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.DEFAULT_MAX_TIME_TO_CLEAR_QUEUE;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.MAX_MANAGEABLE_QUEUED_WORK;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.WRITE_COORDINATION_MAX_CONTRIBUTION_CAPPED_LOG_INTERVAL;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.WRITE_COORDINATION_MAX_CONTRIBUTION_CAP_RATIO;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.WRITE_COORDINATION_UNCAPPING_THRESHOLD;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestLoadProbe.calculateIngestionLoadForExecutor;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestMetricsServiceTests.createShutdownMetadata;
import static org.elasticsearch.xpack.stateless.autoscaling.indexing.NodeIngestionLoadTracker.INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class IngestLoadProbeTests extends ESTestCase {

    public void testCalculateIngestionLoadForExecutor() {
        final TimeValue maxTimeToClearQueue = TimeValue.timeValueSeconds(10);
        final int maxThreads = between(1, 16);
        // Initially average task execution time is 0.0.
        double randomQueueContribution = randomDoubleBetween(0.1, 100.0, true);
        assertThat(
            calculateIngestionLoadForExecutor("test", 0.0, 0.0, 0, maxThreads, maxTimeToClearQueue, ZERO, randomQueueContribution).total(),
            closeTo(0.0, 1e-3)
        );
        // When there is nothing in the queue, we'd still want to keep up with average load
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                1.0,
                timeValueMillis(100).nanos(),
                0,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomQueueContribution
            ).total(),
            closeTo(1.0, 1e-3)
        );
        // A threadpool of 2 with average task time of 100ms can run 200 tasks per 10 seconds.
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                0.0,
                timeValueMillis(100).nanos(),
                100,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(1.0, 100.0, true)
            ).total(),
            closeTo(1.00, 1e-3)
        );
        // We have 1 task in the queue, we'd need roughly 1/100th of a thread more since each thread can do 100 tasks
        // per maxTimeToClearQueue period.
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                1.0,
                timeValueMillis(100).nanos(),
                1,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(0.01, 100, true)
            ).total(),
            closeTo(1.01, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                1.0,
                timeValueMillis(100).nanos(),
                100,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(1.00, 100, true)
            ).total(),
            closeTo(2.00, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                2.0,
                timeValueMillis(100).nanos(),
                200,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(2.00, 100, true)
            ).total(),
            closeTo(4.00, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                2.0,
                timeValueMillis(100).nanos(),
                400,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(4.00, 100, true)
            ).total(),
            closeTo(6.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor(
                "test",
                2.0,
                timeValueMillis(100).nanos(),
                1000,
                maxThreads,
                maxTimeToClearQueue,
                ZERO,
                randomDoubleBetween(10.00, 100, true)
            ).total(),
            closeTo(12.0, 1e-3)
        );
        assertThat(
            calculateIngestionLoadForExecutor("test", 2.0, timeValueMillis(100).nanos(), 1000, maxThreads, maxTimeToClearQueue, ZERO, 4.00)
                .total(),
            closeTo(6.0, 1e-3)
        );
    }

    public void testGetIngestionLoad() {
        Map<String, ExecutorStats> statsPerExecutor = new HashMap<>();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        AtomicLong nowInMillis = new AtomicLong(randomNonNegativeLong());
        var ingestLoadProbe = new IngestLoadProbe(clusterSettings, statsPerExecutor::get, TimeProviderUtils.create(nowInMillis::get));
        var indexShard = Mockito.mock(IndexShard.class);
        ingestLoadProbe.beforeIndexShardRecovery(indexShard, null, ActionListener.noop());

        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.WRITE_COORDINATION, new ExecutorStats(3.0, timeValueMillis(200).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.SYSTEM_WRITE_COORDINATION, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0.0, between(1, 10)));
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(11.0, 1e-3));
        // Exclude coordination executors and the reported total should drop
        clusterSettings.applySettings(Settings.builder().put(INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED.getKey(), false).build());
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(6.0, 1e-3));
        // The individual executor stats are recorded regardless
        assertThat(
            ingestLoadProbe.getNodeIngestionLoad().executorIngestionLoads().get(Names.WRITE_COORDINATION).total(),
            closeTo(3.0, 1e-3)
        );
        assertThat(
            ingestLoadProbe.getNodeIngestionLoad().executorIngestionLoads().get(Names.SYSTEM_WRITE_COORDINATION).total(),
            closeTo(2.0, 1e-3)
        );

        statsPerExecutor.clear();
        // With 200ms per task each thread can do 5 tasks per second
        var queueEmpty = randomBoolean();
        int queueSize = queueEmpty ? 0 : 5 * (int) MAX_TIME_TO_CLEAR_QUEUE.getDefault(Settings.EMPTY).seconds();
        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), queueSize, queueSize, 1));
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0.0, 1));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0, 0.0, 1));
        statsPerExecutor.put(Names.WRITE_COORDINATION, new ExecutorStats(3.0, timeValueMillis(200).nanos(), queueSize, queueSize, 1));
        statsPerExecutor.put(Names.SYSTEM_WRITE_COORDINATION, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0, 1));
        var expectedExtraThreads = queueEmpty ? 0.0 : 1.0;
        // No contribution to total ingestion load from queueing (since we're within the
        // DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION) and no contribution from coordination executors since they are excluded
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(6.0, 1e-3));
        nowInMillis.addAndGet(DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.millis() + randomNonNegativeLong());
        // Queue contribution is now included for non-coordination write executors
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(6.0 + expectedExtraThreads, 1e-3));
        // The individual executor stats are recorded regardless
        assertThat(
            ingestLoadProbe.getNodeIngestionLoad().executorIngestionLoads().get(Names.WRITE_COORDINATION).total(),
            closeTo(3.0 + expectedExtraThreads, 1e-3)
        );
        assertThat(
            ingestLoadProbe.getNodeIngestionLoad().executorIngestionLoads().get(Names.SYSTEM_WRITE_COORDINATION).total(),
            closeTo(2.0, 1e-3)
        );
        // Include coordination executors and the reported total should rise
        clusterSettings.applySettings(Settings.builder().put(INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED.getKey(), true).build());
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(11.0 + expectedExtraThreads * 2, 1e-3));

        final var maxManageableQueuedWork = timeValueSeconds(between(5, 10));
        var averageTaskExecutionTime = timeValueMillis(200);
        var maxThreads = between(1, 3);
        var manageableQueueSize = maxThreads * (maxManageableQueuedWork.millis() / averageTaskExecutionTime.millis());
        statsPerExecutor.clear();
        queueSize = between(0, (int) manageableQueueSize);
        clusterSettings.applySettings(Settings.builder().put(MAX_MANAGEABLE_QUEUED_WORK.getKey(), ZERO).build());
        // before updating the setting, we request extra threads for all the queued work
        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, averageTaskExecutionTime.nanos(), queueSize, queueSize, maxThreads));
        statsPerExecutor.put(
            Names.WRITE_COORDINATION,
            new ExecutorStats(2.0, averageTaskExecutionTime.nanos(), queueSize, queueSize, maxThreads)
        );
        statsPerExecutor.put(Names.SYSTEM_WRITE, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, new ExecutorStats(1.0, timeValueMillis(25).nanos(), 0, 0.0, between(1, 10)));
        statsPerExecutor.put(Names.SYSTEM_WRITE_COORDINATION, new ExecutorStats(2.0, timeValueMillis(70).nanos(), 0, 0.0, between(1, 10)));
        var queueThreadsNeeded = queueSize / ((double) DEFAULT_MAX_TIME_TO_CLEAR_QUEUE.millis() / averageTaskExecutionTime.millis());
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(10.0 + 2 * queueThreadsNeeded, 1e-3));

        clusterSettings.applySettings(Settings.builder().put(MAX_MANAGEABLE_QUEUED_WORK.getKey(), maxManageableQueuedWork).build());
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(10.0, 1e-3));
    }

    public void testQueueReportedAfterFirstShardAssignment() {
        Map<String, ExecutorStats> statsPerExecutor = new HashMap<>();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        AtomicLong nowInMillis = new AtomicLong(randomNonNegativeLong());
        var ingestLoadProbe = new IngestLoadProbe(clusterSettings, statsPerExecutor::get, TimeProviderUtils.create(nowInMillis::get));
        var nodeHasItsFirstShard = randomBoolean();
        var indexShard = Mockito.mock(IndexShard.class);
        if (nodeHasItsFirstShard) {
            ingestLoadProbe.beforeIndexShardRecovery(indexShard, null, ActionListener.noop());
        }
        var emptyStats = new ExecutorStats(0, timeValueMillis(200).nanos(), 0, 0, 1);
        // each 5 tasks take a second, so the following queue size asks for 1.0 extra threads.
        int queueSize = 5 * (int) MAX_TIME_TO_CLEAR_QUEUE.getDefault(Settings.EMPTY).seconds();
        statsPerExecutor.put(Names.WRITE, new ExecutorStats(3.0, timeValueMillis(200).nanos(), queueSize, queueSize, 1));
        statsPerExecutor.put(Names.SYSTEM_WRITE, emptyStats);
        statsPerExecutor.put(Names.SYSTEM_CRITICAL_WRITE, emptyStats);
        statsPerExecutor.put(Names.WRITE_COORDINATION, emptyStats);
        statsPerExecutor.put(Names.SYSTEM_WRITE_COORDINATION, emptyStats);
        // Initially, we ignore queue contribution
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(3.0, 1e-3));
        // After the initial interval, if the node has seen its first shard assignment, we include queue contribution
        nowInMillis.addAndGet(DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.millis() + randomNonNegativeLong());
        assertThat(ingestLoadProbe.getNodeIngestionLoad().totalIngestionLoad(), closeTo(3.0 + (nodeHasItsFirstShard ? 1.0 : 0.0), 1e-3));
    }

    public void testIntervals() {
        final var nodeRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);
        var node0 = DiscoveryNodeUtils.builder("node-0").roles(nodeRoles).build();
        var node1 = DiscoveryNodeUtils.builder("node-1").roles(nodeRoles).build();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(node0).add(node1);
        builder.localNodeId(node0.getId()).masterNodeId(node0.getId());
        final var initialClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(builder).build();

        final TimeValue interval = randomTimeValue(0, 5, TimeUnit.MINUTES);

        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder()
                .put(INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE.getKey(), interval)
                .put(INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE.getKey(), interval)
                .put(INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.getKey(), interval)
                .build()
        );
        AtomicLong now = new AtomicLong(randomLong());
        final var executorStats = randomExecutorStats();
        var ingestLoadProbe = new IngestLoadProbe(clusterSettings, executorStats::get, TimeProviderUtils.create(now::get));
        ingestLoadProbe.clusterChanged(new ClusterChangedEvent("initial", initialClusterState, ClusterState.EMPTY_STATE));

        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(interval.millis() > 0));
        assertThat(ingestLoadProbe.dropQueueDueToInitialStartingInterval(), equalTo(interval.millis() > 0));
        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(false));
        // gets first shard
        var indexShard = Mockito.mock(IndexShard.class);
        ingestLoadProbe.beforeIndexShardRecovery(indexShard, null, ActionListener.noop());
        // might get a shutdown marker
        final var shutdownMarkerAdded = randomBoolean();
        ClusterState clusterStateWithShutdowns = null;
        if (shutdownMarkerAdded) {
            final var shutdownMetadata = createShutdownMetadata(List.of(node1));
            clusterStateWithShutdowns = ClusterState.builder(initialClusterState)
                .metadata(Metadata.builder(initialClusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata))
                .build();
            ingestLoadProbe.clusterChanged(new ClusterChangedEvent("shutdown added", clusterStateWithShutdowns, initialClusterState));

        }
        if (interval.millis() == 0) {
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(false));
            assertThat(ingestLoadProbe.dropQueueDueToInitialStartingInterval(), equalTo(false));
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(false));
            now.addAndGet(randomNonNegativeLong());
        } else {
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(true));
            assertThat(ingestLoadProbe.dropQueueDueToInitialStartingInterval(), equalTo(true));
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(shutdownMarkerAdded));
            // advance time randomly but not beyond the interval
            long toAdvance = randomLongBetween(1, interval.millis() - 1);
            now.addAndGet(toAdvance);
            if (randomBoolean()) {
                ingestLoadProbe.beforeIndexShardRecovery(indexShard, null, ActionListener.noop());
            }
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(true));
            assertThat(ingestLoadProbe.dropQueueDueToInitialStartingInterval(), equalTo(true));
            assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(shutdownMarkerAdded));
            // even if the node leaves, we still continue to consider within the scaling window
            if (shutdownMarkerAdded && randomBoolean()) {
                final var clusterStateWithNodeGone = ClusterState.builder(clusterStateWithShutdowns).nodes(builder.remove(node1)).build();
                ingestLoadProbe.clusterChanged(new ClusterChangedEvent("node left", clusterStateWithNodeGone, clusterStateWithShutdowns));
                assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(true));
            }
            toAdvance = interval.millis() - toAdvance + randomLongBetween(0, 1000);
            now.addAndGet(toAdvance);
        }
        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(false));
        assertThat(ingestLoadProbe.dropQueueDueToInitialStartingInterval(), equalTo(false));
        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(false));
    }

    public void testLastStableAverageTaskExecutionTime() {
        // initial cluster state with two nodes
        final var nodeRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);
        final var initialNodes = IntStream.range(0, 2)
            .mapToObj(i -> DiscoveryNodeUtils.builder("node-" + i).roles(nodeRoles).build())
            .toList();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        initialNodes.forEach(builder::add);
        builder.localNodeId(initialNodes.get(0).getId()).masterNodeId(initialNodes.get(0).getId());
        final var initialClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(builder).build();

        final TimeValue initialIntervalToConsiderNodeAvgTaskExecTimeUnstable = randomTimeValue(1, 5, TimeUnit.MINUTES);
        final TimeValue initialScalingWindowToConsiderAvgTaskExecTimesUnstable = randomTimeValue(1, 5, TimeUnit.MINUTES);
        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder()
                .put(
                    INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE.getKey(),
                    initialIntervalToConsiderNodeAvgTaskExecTimeUnstable
                )
                .put(
                    INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE.getKey(),
                    initialScalingWindowToConsiderAvgTaskExecTimesUnstable
                )
                .build()
        );
        final var expectedWriteExecutorEntries = AverageWriteLoadSampler.WRITE_EXECUTORS.size();
        final var currentTimeMillis = TimeValue.nsecToMSec(Math.abs(System.nanoTime())); // technically System.nanoTime() can be negative
        AtomicLong now = new AtomicLong(randomLongBetween(-currentTimeMillis, currentTimeMillis));
        final var executorStats = randomExecutorStats();
        var ingestLoadProbe = new IngestLoadProbe(clusterSettings, executorStats::get, TimeProviderUtils.create(now::get));
        var indexShard = Mockito.mock(IndexShard.class);
        ingestLoadProbe.clusterChanged(new ClusterChangedEvent("initial", initialClusterState, ClusterState.EMPTY_STATE));

        final var ingestionLoad0 = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(ingestionLoad0.totalIngestionLoad(), greaterThan(0.0));
        assertThat(ingestionLoad0.executorStats().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad0.executorIngestionLoads().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad0.lastStableAvgTaskExecutionTimes().size(), equalTo(0));

        ingestLoadProbe.beforeIndexShardRecovery(indexShard, null, ActionListener.noop());

        final var ingestionLoad1 = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(ingestionLoad1.totalIngestionLoad(), greaterThan(0.0));
        assertThat(ingestionLoad1.executorStats().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad1.executorIngestionLoads().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad1.lastStableAvgTaskExecutionTimes().size(), equalTo(0));

        now.addAndGet(initialIntervalToConsiderNodeAvgTaskExecTimeUnstable.millis() + randomLongBetween(0, 10000));
        final var ingestionLoad2 = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(ingestionLoad2.totalIngestionLoad(), greaterThan(0.0));
        assertThat(ingestionLoad2.executorStats().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad2.executorIngestionLoads().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad2.lastStableAvgTaskExecutionTimes().size(), equalTo(expectedWriteExecutorEntries));
        ingestionLoad2.lastStableAvgTaskExecutionTimes()
            .forEach((executor, execTime) -> assertThat(execTime, equalTo(executorStats.get(executor).averageTaskExecutionNanosEWMA())));

        // Publish during scaling window, last stable avg task execution times should not get updated
        final var shutdownMetadata = createShutdownMetadata(initialNodes);
        final var clusterStateWithShutdowns = ClusterState.builder(initialClusterState)
            .metadata(Metadata.builder(initialClusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata))
            .build();
        ingestLoadProbe.clusterChanged(new ClusterChangedEvent("shutdown added", clusterStateWithShutdowns, initialClusterState));
        final var previousExecutorStats = new HashMap<>(executorStats);
        executorStats.clear();
        executorStats.putAll(randomExecutorStats());
        final var ingestionLoad3 = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(ingestionLoad3.totalIngestionLoad(), greaterThan(0.0));
        assertThat(ingestionLoad3.executorStats(), equalTo(executorStats));
        assertThat(ingestionLoad3.executorIngestionLoads().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad3.lastStableAvgTaskExecutionTimes().size(), equalTo(expectedWriteExecutorEntries));
        // The last stable avg task execution times should remain unchanged
        ingestionLoad3.lastStableAvgTaskExecutionTimes()
            .forEach(
                (executor, execTime) -> assertThat(execTime, equalTo(previousExecutorStats.get(executor).averageTaskExecutionNanosEWMA()))
            );

        // After both intervals, last stable avg task execution times should get updated
        now.addAndGet(
            Math.max(
                initialScalingWindowToConsiderAvgTaskExecTimesUnstable.millis(),
                initialIntervalToConsiderNodeAvgTaskExecTimeUnstable.millis()
            ) + randomLongBetween(0, 10000)
        );
        final var ingestionLoad4 = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(ingestionLoad4.totalIngestionLoad(), greaterThan(0.0));
        assertThat(ingestionLoad4.executorStats().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad4.executorIngestionLoads().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestionLoad4.lastStableAvgTaskExecutionTimes().size(), equalTo(expectedWriteExecutorEntries));
        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToScaling(), equalTo(false));
        assertThat(ingestLoadProbe.considerTaskExecutionTimeUnstableDueToInitialStart(), equalTo(false));
        ingestionLoad4.lastStableAvgTaskExecutionTimes()
            .forEach((executor, execTime) -> assertThat(execTime, equalTo(executorStats.get(executor).averageTaskExecutionNanosEWMA())));
    }

    public void testGetCappedWriteCoordinationExecutorIngestionLoad() {
        int ratio = randomIntBetween(1, 10);
        int writeCoordinationUncappingThreshold = randomIntBetween(5, 10);
        ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder()
                .put(WRITE_COORDINATION_MAX_CONTRIBUTION_CAP_RATIO.getKey(), ratio)
                .put(INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED.getKey(), "true")
                .put(WRITE_COORDINATION_UNCAPPING_THRESHOLD.getKey(), writeCoordinationUncappingThreshold)
                .build()
        );

        final var executorStats = randomExecutorStats();
        ExecutorStats writeExecutorStats = executorStats.get(Names.WRITE);

        final var currentTimeMillis = TimeValue.nsecToMSec(Math.abs(System.nanoTime()));
        AtomicLong now = new AtomicLong(randomLongBetween(-currentTimeMillis, currentTimeMillis));
        var ingestLoadProbe = new IngestLoadProbe(clusterSettings, executorStats::get, TimeProviderUtils.create(now::get));

        ExecutorStats idlingWriteExecutorStats = new ExecutorStats(
            writeCoordinationUncappingThreshold - randomIntBetween(2, 3),
            writeExecutorStats.averageTaskExecutionNanosEWMA(),
            writeExecutorStats.currentQueueSize(),
            writeCoordinationUncappingThreshold - randomIntBetween(2, 3),
            writeExecutorStats.maxThreads()
        );
        executorStats.put(Names.WRITE, idlingWriteExecutorStats);

        IngestionLoad.NodeIngestionLoad nodeIngestionLoad = ingestLoadProbe.getNodeIngestionLoad();
        // Case 1: No capping when write load is low
        assertThat(
            nodeIngestionLoad.executorIngestionLoads().get(Names.WRITE).total(),
            lessThan((double) writeCoordinationUncappingThreshold)
        );
        verifyWriteCoordinationExecutorIngestionTotal(
            nodeIngestionLoad.executorIngestionLoads().get(Names.WRITE_COORDINATION).total(),
            nodeIngestionLoad
        );

        // Case 2: No capping when write coordination is lower than 10x write load
        ExecutorStats aboveUncappingThresholdWriteExecutorStats = new ExecutorStats(
            writeCoordinationUncappingThreshold + randomIntBetween(2, 3),
            writeExecutorStats.averageTaskExecutionNanosEWMA(),
            writeCoordinationUncappingThreshold + randomIntBetween(2, 3),
            writeCoordinationUncappingThreshold + randomIntBetween(2, 3),
            writeExecutorStats.maxThreads()
        );

        executorStats.put(Names.WRITE, aboveUncappingThresholdWriteExecutorStats);
        ExecutorStats moderateWriteCoordinationStats = new ExecutorStats(
            aboveUncappingThresholdWriteExecutorStats.averageLoad() * (ratio - 1),
            aboveUncappingThresholdWriteExecutorStats.averageTaskExecutionNanosEWMA(),
            aboveUncappingThresholdWriteExecutorStats.currentQueueSize() * (ratio - 1),
            aboveUncappingThresholdWriteExecutorStats.averageQueueSize() * (ratio - 1),
            aboveUncappingThresholdWriteExecutorStats.maxThreads()
        );
        executorStats.put(Names.WRITE_COORDINATION, moderateWriteCoordinationStats);
        nodeIngestionLoad = ingestLoadProbe.getNodeIngestionLoad();
        assertThat(
            nodeIngestionLoad.executorIngestionLoads().get(Names.WRITE_COORDINATION).total(),
            lessThan(nodeIngestionLoad.executorIngestionLoads().get(Names.WRITE).total() * ratio)
        );

        verifyWriteCoordinationExecutorIngestionTotal(
            nodeIngestionLoad.executorIngestionLoads().get(Names.WRITE_COORDINATION).total(),
            nodeIngestionLoad
        );

        // Case 3: Extreme write coordination activities greater than 10 times write executor ingestion load.
        ExecutorStats writeCoordinationStats = new ExecutorStats(
            aboveUncappingThresholdWriteExecutorStats.averageLoad() * randomIntBetween(ratio + 1, 200),
            aboveUncappingThresholdWriteExecutorStats.averageTaskExecutionNanosEWMA(),
            aboveUncappingThresholdWriteExecutorStats.currentQueueSize() * randomIntBetween(100, 200),
            aboveUncappingThresholdWriteExecutorStats.averageQueueSize() * randomIntBetween(100, 200),
            aboveUncappingThresholdWriteExecutorStats.maxThreads()
        );
        executorStats.put(Names.WRITE_COORDINATION, writeCoordinationStats);

        MockLog.assertThatLogger(() -> {
            IngestionLoad.NodeIngestionLoad nodeIngestionLoadWithBusyWriteCoordination = ingestLoadProbe.getNodeIngestionLoad();
            var capped = nodeIngestionLoadWithBusyWriteCoordination.executorIngestionLoads().get(Names.WRITE).total() * ratio;
            verifyWriteCoordinationExecutorIngestionTotal(capped, nodeIngestionLoadWithBusyWriteCoordination);
        },
            IngestLoadProbe.class,
            new MockLog.SeenEventExpectation(
                "capped warning",
                IngestLoadProbe.class.getCanonicalName(),
                Level.WARN,
                "write coordination thread pool ingest load * is capped to * which is write ingest load * cap ratio *"
            )
        );
    }

    private void verifyWriteCoordinationExecutorIngestionTotal(double expected, IngestionLoad.NodeIngestionLoad nodeIngestionLoad) {
        double nonWriteCoordinationIngestLoadTotal = AverageWriteLoadSampler.WRITE_EXECUTORS.stream()
            .filter(executor -> executor.equals(Names.WRITE_COORDINATION) == false)
            .map(executor -> nodeIngestionLoad.executorIngestionLoads().get(executor).total())
            .reduce(Double::sum)
            .get();

        // Java double uses binary floating-point, so most decimal numbers cannot be represented exactly.
        assertEquals(nodeIngestionLoad.totalIngestionLoad(), nonWriteCoordinationIngestLoadTotal + expected, 1e-9);
    }

    private static Map<String, ExecutorStats> randomExecutorStats() {
        return AverageWriteLoadSampler.WRITE_EXECUTORS.stream()
            .collect(
                Collectors.toMap(
                    executorName -> executorName,
                    executorName -> new ExecutorStats(
                        randomDoubleBetween(0, 100, true),
                        randomTimeValue(10, 10000, TimeUnit.MILLISECONDS).nanos(),
                        randomInt(1000),
                        randomDoubleBetween(0, 500, true),
                        randomIntBetween(1, 64)
                    )
                )
            );
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                MAX_TIME_TO_CLEAR_QUEUE,
                MAX_QUEUE_CONTRIBUTION_FACTOR,
                INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED,
                INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION,
                MAX_MANAGEABLE_QUEUED_WORK,
                INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE,
                INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE,
                WRITE_COORDINATION_MAX_CONTRIBUTION_CAP_RATIO,
                WRITE_COORDINATION_MAX_CONTRIBUTION_CAPPED_LOG_INTERVAL,
                WRITE_COORDINATION_UNCAPPING_THRESHOLD
            )
        );
    }
}
