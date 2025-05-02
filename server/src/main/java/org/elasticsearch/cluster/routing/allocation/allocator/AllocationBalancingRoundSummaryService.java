/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the lifecycle of a series of {@link BalancingRoundSummary} results from allocation balancing rounds and creates reports thereof.
 * Reporting balancer round summary results will provide information with which to do cost-benefit analyses of the work that shard
 * allocation rebalancing executes.
 *
 * Any successfully added summary via {@link #addBalancerRoundSummary(BalancingRoundSummary)} will eventually be collected/drained and
 * reported. This should still be done in the event of the node stepping down from master, on the assumption that all summaries are only
 * added while master and should be drained for reporting. There is no need to start/stop this service with master election/stepdown because
 * balancer rounds will no longer be supplied when not master. It will simply drain the last summaries and then have nothing more to do.
 * This does have the tradeoff that non-master nodes will run a task to check for summaries to report every
 * {@link #BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING} seconds.
 */
public class AllocationBalancingRoundSummaryService {

    /** Turns on or off balancing round summary reporting. */
    public static final Setting<Boolean> ENABLE_BALANCER_ROUND_SUMMARIES_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.desired_balance.enable_balancer_round_summaries",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Controls how frequently in time balancer round summaries are logged. */
    public static final Setting<TimeValue> BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.balanace_round_summaries_interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(AllocationBalancingRoundSummaryService.class);
    private final ThreadPool threadPool;
    private volatile boolean enableBalancerRoundSummaries;
    private volatile TimeValue summaryReportInterval;

    /**
     * A concurrency-safe list of balancing round summaries. Balancer rounds are run and added here serially, so the queue will naturally
     * progress from newer to older results.
     */
    private final ConcurrentLinkedQueue<BalancingRoundSummary> summaries = new ConcurrentLinkedQueue<>();

    /** This reference is set when reporting is scheduled. If it is null, then reporting is inactive. */
    private final AtomicReference<Scheduler.Cancellable> scheduledReportFuture = new AtomicReference<>();

    public AllocationBalancingRoundSummaryService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        this.threadPool = threadPool;
        // Initialize the local setting values to avoid a null access when ClusterSettings#initializeAndWatch is called on each setting:
        // updating enableBalancerRoundSummaries accesses summaryReportInterval.
        this.enableBalancerRoundSummaries = clusterSettings.get(ENABLE_BALANCER_ROUND_SUMMARIES_SETTING);
        this.summaryReportInterval = clusterSettings.get(BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING);

        clusterSettings.initializeAndWatch(ENABLE_BALANCER_ROUND_SUMMARIES_SETTING, value -> {
            this.enableBalancerRoundSummaries = value;
            updateBalancingRoundSummaryReporting();
        });
        clusterSettings.initializeAndWatch(BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING, value -> {
            // The new value will get picked up the next time that the summary report task reschedules itself on the thread pool.
            this.summaryReportInterval = value;
        });
    }

    /**
     * Summarizes the work required to move from an old to new desired balance shard allocation.
     */
    public static BalancingRoundSummary createBalancerRoundSummary(DesiredBalance oldDesiredBalance, DesiredBalance newDesiredBalance) {
        var changes = DesiredBalance.shardAssignmentChanges(oldDesiredBalance, newDesiredBalance);
        return new BalancingRoundSummary(
            createWeightsSummary(oldDesiredBalance, newDesiredBalance),
            changes.movements(),
            changes.newlyAssigned(),
            0, // TODO: WIP ES-10341
            0, // TODO: WIP ES-10341
            changes.newlyUnassigned(),
            changes.shutdownInducedMoves()
        );
    }

    /**
     * Creates a summary of the node weight changes from {@code oldDesiredBalance} to {@code newDesiredBalance}.
     * See {@link BalancingRoundSummary.NodesWeightsChanges} for content details.
     */
    private static Map<String, BalancingRoundSummary.NodesWeightsChanges> createWeightsSummary(
        DesiredBalance oldDesiredBalance,
        DesiredBalance newDesiredBalance
    ) {
        var oldWeightsPerNode = oldDesiredBalance.weightsPerNode();
        var newWeightsPerNode = newDesiredBalance.weightsPerNode();

        Map<String, BalancingRoundSummary.NodesWeightsChanges> nodeNameToWeightInfo = new HashMap<>(oldWeightsPerNode.size());
        for (var nodeAndWeights : oldWeightsPerNode.entrySet()) {
            var discoveryNode = nodeAndWeights.getKey();
            var oldNodeWeightStats = nodeAndWeights.getValue();

            // The node may no longer exists in the new DesiredBalance. If so, the new weights for that node are effectively zero. New
            // weights of zero will result in correctly negative weight diffs for the removed node.
            var newNodeWeightStats = newWeightsPerNode.getOrDefault(discoveryNode, DesiredBalanceMetrics.NodeWeightStats.ZERO);

            nodeNameToWeightInfo.put(
                discoveryNode.getName(),
                new BalancingRoundSummary.NodesWeightsChanges(
                    oldNodeWeightStats,
                    BalancingRoundSummary.NodeWeightsDiff.create(oldNodeWeightStats, newNodeWeightStats)
                )
            );
        }

        // There may be a new node in the new DesiredBalance that was not in the old DesiredBalance. So we'll need to iterate the nodes in
        // the new DesiredBalance to check.
        for (var nodeAndWeights : newWeightsPerNode.entrySet()) {
            var discoveryNode = nodeAndWeights.getKey();
            if (nodeNameToWeightInfo.containsKey(discoveryNode.getName()) == false) {
                // This node is new in the new DesiredBalance, there was no entry added during iteration of the nodes in the old
                // DesiredBalance. So we'll make a new entry with a base of zero value weights and a weights diff of the new node's weights.
                nodeNameToWeightInfo.put(
                    discoveryNode.getName(),
                    new BalancingRoundSummary.NodesWeightsChanges(
                        DesiredBalanceMetrics.NodeWeightStats.ZERO,
                        BalancingRoundSummary.NodeWeightsDiff.create(DesiredBalanceMetrics.NodeWeightStats.ZERO, nodeAndWeights.getValue())
                    )
                );
            }
        }

        return nodeNameToWeightInfo;
    }

    /**
     * Creates and saves a balancer round summary for the work to move from {@code oldDesiredBalance} to {@code newDesiredBalance}. If
     * balancer round summaries are not enabled in the cluster (see {@link #ENABLE_BALANCER_ROUND_SUMMARIES_SETTING}), then the summary is
     * immediately discarded.
     */
    public void addBalancerRoundSummary(DesiredBalance oldDesiredBalance, DesiredBalance newDesiredBalance) {
        addBalancerRoundSummary(createBalancerRoundSummary(oldDesiredBalance, newDesiredBalance));
    }

    /**
     * Adds the summary of a balancing round. If summaries are enabled, this will eventually be reported (logging, etc.). If balancer round
     * summaries are not enabled in the cluster, then the summary is immediately discarded (so as not to fill up a data structure that will
     * never be drained).
     */
    public void addBalancerRoundSummary(BalancingRoundSummary summary) {
        if (enableBalancerRoundSummaries == false) {
            return;
        }

        summaries.add(summary);
    }

    /**
     * Reports on all the balancer round summaries added since the last call to this method, if there are any. Then reschedules itself per
     * the {@link #ENABLE_BALANCER_ROUND_SUMMARIES_SETTING} and {@link #BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING} settings.
     */
    private void reportSummariesAndThenReschedule() {
        drainAndReportSummaries();
        rescheduleReporting();
    }

    /**
     * Drains all the waiting balancer round summaries (if there are any) and reports them.
     */
    private void drainAndReportSummaries() {
        var combinedSummaries = drainSummaries();
        if (combinedSummaries == BalancingRoundSummary.CombinedBalancingRoundSummary.EMPTY_RESULTS) {
            return;
        }

        logger.info("Balancing round summaries: " + combinedSummaries);
    }

    /**
     * Returns a combined summary of all unreported allocation round summaries: may summarize a single balancer round, multiple, or none.
     *
     * @return {@link BalancingRoundSummary.CombinedBalancingRoundSummary#EMPTY_RESULTS} if there are no balancing round summaries waiting
     * to be reported.
     */
    private BalancingRoundSummary.CombinedBalancingRoundSummary drainSummaries() {
        ArrayList<BalancingRoundSummary> batchOfSummaries = new ArrayList<>();
        while (summaries.isEmpty() == false) {
            batchOfSummaries.add(summaries.poll());
        }
        return BalancingRoundSummary.CombinedBalancingRoundSummary.combine(batchOfSummaries);
    }

    /**
     * Schedules a periodic task to drain and report the latest balancer round summaries, or cancels the already running task, if the latest
     * setting values dictate a change to enable or disable reporting. A change to {@link #BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING}
     * will only take effect when the periodic task completes and reschedules itself.
     */
    private void updateBalancingRoundSummaryReporting() {
        if (this.enableBalancerRoundSummaries) {
            startReporting(this.summaryReportInterval);
        } else {
            cancelReporting();
            // Clear the data structure so that we don't retain unnecessary memory.
            drainSummaries();
        }
    }

    /**
     * Schedules a reporting task, if one is not already scheduled. The reporting task will reschedule itself going forward.
     */
    private void startReporting(TimeValue intervalValue) {
        if (scheduledReportFuture.get() == null) {
            scheduleReporting(intervalValue);
        }
    }

    /**
     * Cancels the future reporting task and resets {@link #scheduledReportFuture} to null.
     *
     * Note that this is best-effort: cancellation can race with {@link #rescheduleReporting}. But that is okay because the subsequent
     * {@link #rescheduleReporting} will use the latest settings and choose to cancel reporting if appropriate.
     */
    private void cancelReporting() {
        var future = scheduledReportFuture.getAndSet(null);
        if (future != null) {
            future.cancel();
        }
    }

    private void scheduleReporting(TimeValue intervalValue) {
        scheduledReportFuture.set(
            threadPool.schedule(this::reportSummariesAndThenReschedule, intervalValue, threadPool.executor(ThreadPool.Names.GENERIC))
        );
    }

    /**
     * Looks at the given setting values and decides whether to schedule another reporting task or cancel reporting now.
     */
    private void rescheduleReporting() {
        if (this.enableBalancerRoundSummaries) {
            // It's possible that this races with a concurrent call to cancel reporting, but that's okay. The next rescheduleReporting call
            // will check the latest settings and cancel.
            scheduleReporting(this.summaryReportInterval);
        } else {
            cancelReporting();
        }
    }

    /**
     * Checks that the number of entries in {@link #summaries} matches the given {@code numberOfSummaries} count.
     */
    protected void verifyNumberOfSummaries(int numberOfSummaries) {
        assert numberOfSummaries == summaries.size();
    }

}
