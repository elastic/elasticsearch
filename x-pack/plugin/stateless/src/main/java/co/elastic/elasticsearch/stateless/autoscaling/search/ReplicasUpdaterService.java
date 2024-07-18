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

import co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.IndexRankingProperties;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;

public class ReplicasUpdaterService extends AbstractLifecycleComponent implements LocalNodeMasterListener {
    private static final Logger LOGGER = LogManager.getLogger(ReplicasUpdaterService.class);

    /**
     * Setting controlling the frequency in which this service pulls for new replica update suggestions.
     * Defaults to 5 minutes.
     */
    public static final Setting<TimeValue> REPLICA_UPDATER_INTERVAL = Setting.timeSetting(
        "serverless.autoscaling.replica_updater_sample_interval",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting controls how many repeated scale down signals we need to receive in order
     * to actually perform a scaling down of the replica setting of an index to 1.
     * Defaults to 6, meaning that with an {@link #REPLICA_UPDATER_INTERVAL} of 5 minutes this
     * means that we need to see a scale down signal for 30 min before we scale back.
     */
    public static final Setting<Integer> REPLICA_UPDATER_SCALEDOWN_REPETITIONS = Setting.intSetting(
        "serverless.autoscaling.replica_updater_scaledown_repetitions",
        6,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Search power setting below which there should be only 1 replica for all interactive indices
     */
    public static final int SEARCH_POWER_MIN_NO_REPLICATION = 100;

    /**
     * Search power setting at which we want to replicate all interactive indices with a factor of 2
     */
    public static final int SEARCH_POWER_MIN_FULL_REPLICATION = 250;

    private final NodeClient client;
    private final ClusterService clusterService;
    private final SearchMetricsService searchMetricsService;
    /**
     * Counters used to prevent immediate scale-down actions to prevent scaling down
     * indices with frequently changing scaling decisions.
     */
    final Map<String, AtomicInteger> scaleDownCounters = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    // guard flag to prevent running the scheduled job in parallel when e.g. canceling
    // the old job hasn't completed yet
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile boolean pendingScaleDownAfterDisabling = false;
    private volatile boolean enableReplicasForInstantFailover;
    private volatile Cancellable job;
    private volatile TimeValue updateInterval;
    private volatile Integer scaledownRepetitionSetting;

    public ReplicasUpdaterService(
        ThreadPool threadPool,
        ClusterService clusterService,
        NodeClient client,
        SearchMetricsService searchMetricsService
    ) {
        this.threadPool = threadPool;
        this.client = client;
        this.searchMetricsService = searchMetricsService;
        this.clusterService = clusterService;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_FOR_INSTANT_FAILOVER, this::updateEnableReplicasForInstantFailover);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_INTERVAL, this::setInterval);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_SCALEDOWN_REPETITIONS, this::setScaledownRepetitionSetting);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
    }

    void setScaledownRepetitionSetting(Integer scaledownRepetitionSetting) {
        this.scaledownRepetitionSetting = scaledownRepetitionSetting;
    }

    void setInterval(TimeValue newValue) {
        LOGGER.info("updating [{}] setting from [{}] to [{}]", REPLICA_UPDATER_INTERVAL.getKey(), this.updateInterval, newValue);
        this.updateInterval = newValue;

        // Only re-schedule if we've been scheduled, this should only be the case on elected master node.
        if (job != null) {
            // we want to keep state here because we only change the update frequency of the task
            unschedule(false);
            scheduleTask();
        }
    }

    // pkg private for testing
    TimeValue getInterval() {
        return this.updateInterval;
    }

    void updateEnableReplicasForInstantFailover(Boolean value) {
        this.enableReplicasForInstantFailover = value;
        if (value) {
            LOGGER.info("enabling replicas for instant failover");
        } else {
            LOGGER.info("disabling replicas for instant failover");
            // if we are on a node with a scheduled job, scale everything down with the next job cycle
            this.pendingScaleDownAfterDisabling = true;
            this.scaleDownCounters.clear();
        }
    }

    void updateSearchPowerMin(Integer spMin) {
        if (enableReplicasForInstantFailover && job != null) {
            performReplicaUpdates(true);
        }
    }

    // pkg private for testing
    Cancellable getJob() {
        return this.job;
    }

    /**
     * This method calculates which indices require a change in their current replica
     * setting based on the current Search Power (SP) setting.
     * We should not have to call this method for SP &lt; 100, this case is already fully handled in {@link #performReplicaUpdates}.
     * For SP >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} all indices get two replicas.
     * For settings between those values, we rank indices and give part of them two replicas.
     */
    Map<Integer, Set<String>> getRecommendedReplicaChanges(ReplicaRankingContext rankingContext) {
        assert rankingContext.getSearchPowerMin() >= 100 : "we should not have to call this method for SP < 100";
        Map<Integer, Set<String>> numReplicaChanges = new HashMap<>(2);
        LOGGER.trace("Calculating index replica recommendations for " + rankingContext.indices());
        if (rankingContext.getSearchPowerMin() >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            for (IndexRankingProperties properties : rankingContext.properties()) {
                int replicas = properties.indexProperties().replicas();
                if (properties.isInteractive()) {
                    if (replicas != 2) {
                        setNumReplicasForIndex(properties.indexProperties().name(), 2, numReplicaChanges);
                    }
                } else {
                    if (replicas != 1) {
                        setNumReplicasForIndex(properties.indexProperties().name(), 1, numReplicaChanges);
                    }
                }
            }
        } else {
            // search power should be between 100 and 250 here
            Set<String> twoReplicaEligibleIndices = getRankedIndicesBelowThreshold(
                rankingContext.properties(),
                rankingContext.getThreshold()
            );
            for (var rankedIndex : rankingContext.properties()) {
                String indexName = rankedIndex.indexProperties().name();
                int replicas = rankedIndex.indexProperties().replicas();
                if (twoReplicaEligibleIndices.contains(indexName)) {
                    assert rankedIndex.isInteractive() : "only interactive indices should get additional copies";
                    if (replicas != 2) {
                        setNumReplicasForIndex(indexName, 2, numReplicaChanges);
                    }
                } else {
                    if (replicas != 1) {
                        setNumReplicasForIndex(indexName, 1, numReplicaChanges);
                    }
                }
            }
        }
        return numReplicaChanges;
    }

    /**
     * Returns indices that should be scaled back to one replica.
     * This doesn't include indices that according to current statistics in {@link SearchMetricsService} already
     * are set to one replica.
     */
    private Set<String> resetReplicasForAllIndices() {
        Set<String> indicesToScaleBack = new HashSet<>();
        Map<Index, IndexProperties> indicesMap = this.searchMetricsService.getIndices();
        for (Map.Entry<Index, IndexProperties> entry : indicesMap.entrySet()) {
            Index index = entry.getKey();
            IndexProperties settings = entry.getValue();
            if (settings.replicas() != 1) {
                indicesToScaleBack.add(index.getName());
            }
        }
        return indicesToScaleBack;
    }

    private static void setNumReplicasForIndex(String index, int numReplicas, Map<Integer, Set<String>> numReplicaChanges) {
        numReplicaChanges.compute(numReplicas, (integer, strings) -> {
            if (strings == null) {
                strings = new HashSet<>();
            }
            strings.add(index);
            return strings;
        });
    }

    private void publishUpdateReplicaSetting(int numReplicasTarget, Set<String> indices) {
        if (ensureRunning() == false) {
            // break out and clear counters if some other thread canceled the job at this point.
            return;
        }
        LOGGER.trace("Publishing update to " + numReplicasTarget + " replicas for " + indices);
        if (indices.isEmpty() == false) {
            TransportUpdateReplicasAction.Request request = new TransportUpdateReplicasAction.Request(
                numReplicasTarget,
                indices.toArray(new String[0])
            );
            client.executeLocally(TransportUpdateReplicasAction.TYPE, request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    scaleDownCounters.entrySet().removeIf(e -> indices.contains(e.getKey()));
                    LOGGER.debug("Updated replicas for " + indices + " to " + numReplicasTarget);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.warn("Error updating replicas for " + indices + " to " + numReplicasTarget, e);
                }
            });
        }
    }

    /**
     * Scheduled task that gets the last replicas update recommendations and performs a settings update.
     */
    void performReplicaUpdates() {
        performReplicaUpdates(false);
    }

    private void performReplicaUpdates(boolean immediateScaleDown) {
        if (running.compareAndSet(false, true)) {
            try {
                if (checkDisabledAndNeedsScaledown()) {
                    return;
                }

                int indicesScaledDown = 0;
                int indicesScaledUp = 0;
                ReplicaRankingContext rankingContext = searchMetricsService.createRankingContext();
                LOGGER.debug("Ranking context: " + rankingContext);
                if (rankingContext.getSearchPowerMin() < SEARCH_POWER_MIN_NO_REPLICATION) {
                    // we can scale everything down immediately
                    Set<String> indicesToScaleDown = resetReplicasForAllIndices();
                    publishUpdateReplicaSetting(1, indicesToScaleDown);
                    indicesScaledDown = indicesToScaleDown.size();
                } else {
                    Map<Integer, Set<String>> numberOfReplicaChanges = getRecommendedReplicaChanges(rankingContext);
                    // apply scaling up to two replica suggestions immediately
                    Set<String> indicesToScaleUp = numberOfReplicaChanges.remove(2);
                    if (indicesToScaleUp != null) {
                        publishUpdateReplicaSetting(2, indicesToScaleUp);
                        indicesScaledUp = indicesToScaleUp.size();
                    }

                    // Scale down decisions requires a certain number of repetitions to be considered stable.
                    // We want to avoid flapping up/down scaling decisions because if we scale up again soon
                    // the performance cost of this outweighs the cost of keeping two replicas around longer.
                    // This is also necessary for SPmin >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} because
                    // even in that case indices might enter and fall out of the interactive boosting window.
                    Set<String> indicesToScaleDown = numberOfReplicaChanges.remove(1);
                    Set<String> countersInUse = Collections.emptySet();
                    if (indicesToScaleDown != null) {
                        if (ensureRunning() == false) {
                            // break out if some other thread canceled the job at this point.
                            return;
                        }
                        if (immediateScaleDown) {
                            publishUpdateReplicaSetting(1, indicesToScaleDown);
                            indicesScaledDown = indicesToScaleDown.size();
                        } else {
                            Set<String> scaleDownUpdatesToSend = new HashSet<>();
                            countersInUse = indicesToScaleDown;
                            for (String index : indicesToScaleDown) {
                                AtomicInteger scaleDownRepetitions = scaleDownCounters.computeIfAbsent(index, k -> new AtomicInteger(0));
                                if (scaleDownRepetitions.incrementAndGet() >= scaledownRepetitionSetting) {
                                    scaleDownUpdatesToSend.add(index);
                                }
                            }
                            publishUpdateReplicaSetting(1, scaleDownUpdatesToSend);
                            indicesScaledDown = scaleDownUpdatesToSend.size();
                        }
                        // We only need to keep counters for scaling down candidates that haven't been included in this round's
                        // updates, e.g. because they haven't reached the number of repetitions needed for stabilization yet.
                        // We can remove all counters that are not part of this update's indices to scale down.
                        scaleDownCounters.entrySet().removeIf(e -> indicesToScaleDown.contains(e.getKey()) == false);
                    }
                    clearCountersExcept(countersInUse);
                    assert numberOfReplicaChanges.isEmpty() : "we should have processed all requested replica demand changes";
                }

                LOGGER.info(
                    "Finished replicas update task. Indices scaled up: "
                        + indicesScaledUp
                        + ", scaled down: "
                        + indicesScaledDown
                        + ", at SPmin: "
                        + rankingContext.getSearchPowerMin()
                );
            } finally {
                boolean running = this.running.compareAndSet(true, false);
                assert running : "Job should still have been running at this moment";
            }
        } else {
            LOGGER.debug("skip running replicas update task, there is one instance in progress already");
        }
    }

    private boolean checkDisabledAndNeedsScaledown() {
        boolean featureDisabled = this.enableReplicasForInstantFailover == false;
        if (featureDisabled) {
            // we might need to scale down indices if the replica feature was disabled
            if (this.pendingScaleDownAfterDisabling) {
                publishUpdateReplicaSetting(1, resetReplicasForAllIndices());
                this.pendingScaleDownAfterDisabling = false;
            }
        }
        return featureDisabled;
    }

    /**
     * make sure the job isn't cancelled yet, if so we also clear internal state.
     * Returns `true` if we are still running the job.
     */
    private boolean ensureRunning() {
        if (job == null || job.isCancelled()) {
            clearCounters();
            return false;
        }
        return true;
    }

    void scheduleTask() {
        if (job == null) {
            LOGGER.debug("schedule replica updater task");
            job = threadPool.scheduleWithFixedDelay(
                this::performReplicaUpdates,
                this.updateInterval,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            performReplicaUpdates();
        }
    }

    /**
     * This cleans all scale-down counters. These counters are used to
     * delay scale-down actions until we have received
     * {@link #REPLICA_UPDATER_SCALEDOWN_REPETITIONS} repeated asks to
     * scale an index down to one replica. We do this to prevent scaling down indices
     * that receive frequently changing scaling decisions in a short amount of time.
     */
    private void clearCounters() {
        clearCountersExcept(Collections.emptySet());
    }

    private void clearCountersExcept(@Nullable Set<String> countersToKeep) {
        if (countersToKeep == null || countersToKeep.size() == 0) {
            this.scaleDownCounters.clear();
        } else {
            scaleDownCounters.entrySet().removeIf(e -> countersToKeep.contains(e.getKey()) == false);
        }
    }

    private void unschedule(boolean clearState) {
        LOGGER.debug("unschedule replica updater task");
        if (job != null) {
            job.cancel();
            job = null;
        }
        if (clearState) {
            clearCounters();
        }
    }

    @Override
    protected void doStart() {
        this.clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        this.clusterService.removeListener(this);
        unschedule(true);
    }

    @Override
    protected void doClose() throws IOException {
        unschedule(true);
    }

    @Override
    public void onMaster() {
        scheduleTask();
    }

    @Override
    public void offMaster() {
        unschedule(true);
    }

}
