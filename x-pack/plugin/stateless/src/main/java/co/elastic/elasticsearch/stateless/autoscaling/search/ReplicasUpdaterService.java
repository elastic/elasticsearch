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
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
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
    final Map<String, AtomicInteger> scaleDownCounters = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    // guard flag to prevent running the scheduled job in parallel when e.g. canceling
    // the old job hasn't completed yet
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean pendingScaleDownAfterDisabling = false;
    private volatile boolean enableReplicasForInstantFailover;
    private volatile int searchPowerMinSetting;
    private volatile int searchPowerMaxSetting;
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
        clusterSettings.initializeAndWatch(SEARCH_POWER_SETTING, this::updateSearchPower);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MAX_SETTING, this::updateSearchPowerMax);
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

    void updateSearchPower(Integer sp) {
        LOGGER.info("Updating search power to " + sp);
        if (this.searchPowerMinSetting == this.searchPowerMaxSetting) {
            updateSearchPowerMin(sp);
            updateSearchPowerMax(sp);
        } else {
            throw new IllegalArgumentException(
                "Updating "
                    + ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey()
                    + " ["
                    + sp
                    + "] while "
                    + ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                    + " ["
                    + this.searchPowerMinSetting
                    + "] and "
                    + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                    + " ["
                    + this.searchPowerMaxSetting
                    + "] are not equal."
            );
        }
    }

    void updateSearchPowerMin(Integer spMin) {
        this.searchPowerMinSetting = spMin;
        if (enableReplicasForInstantFailover && job != null) {
            performReplicaUpdates();
        }
    }

    void updateSearchPowerMax(Integer spMax) {
        this.searchPowerMaxSetting = spMax;
    }

    // pkg private for testing
    Cancellable getJob() {
        return this.job;
    }

    /**
     * Get all indices minus system indices and indices that have 'auto_expand_replicas' enabled.
     * TODO remove these filters once we decide how to apporach system indices and auto_expand_replica settings
     */
    Map<Index, IndexProperties> getFilteredIndices() {
        ConcurrentMap<Index, IndexProperties> indices = this.searchMetricsService.getIndices();
        Map<Index, IndexProperties> filteredIndices = indices.entrySet().stream().filter(e -> {
            if (e.getValue().isSystem()) {
                return false;
            }
            return true;
        }).filter(e -> {
            if (e.getValue().isAutoExpandReplicas()) {
                return false;
            }
            return true;
        }).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
        return filteredIndices;
    }

    /**
     * This method calculates which indices require a change in their current replica
     * setting based on the current Search Power (SP) setting.
     * We should not have to call this method for SP &lt; 100, this case is already fully handled in {@link #performReplicaUpdates}.
     * For SP >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} all indices get two replicas.
     * For settings between those values, we rank indices and give part of them two replicas.
     */
    Map<Integer, Set<String>> getRecommendedReplicaChanges() {
        assert this.searchPowerMinSetting >= 100 : "we should not have to call this method for SP < 100";
        Map<Integer, Set<String>> numReplicaChanges = new HashMap<>(2);
        Map<Index, IndexProperties> indicesMap = this.getFilteredIndices();
        Map<ShardId, SearchMetricsService.ShardMetrics> shardMetricsMap = this.searchMetricsService.getShardMetrics();
        LOGGER.trace("Calculating index replica recommendations for " + indicesMap.keySet());
        if (searchPowerMinSetting >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            for (Map.Entry<Index, IndexProperties> entry : indicesMap.entrySet()) {
                Index index = entry.getKey();
                IndexProperties indexProperties = entry.getValue();
                boolean indexInteractive = false;
                for (int i = 0; i < indexProperties.shards(); i++) {
                    SearchMetricsService.ShardMetrics shardMetrics = shardMetricsMap.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // continue with the next shard, this one might be removed or have 0 replicas but it
                        // can also be from a tiny index with skewed document distribution and some shards
                        // not having any documents in them
                        continue;
                    }
                    if (shardMetrics.shardSize.interactiveSizeInBytes() > 0) {
                        indexInteractive = true;
                        break;
                    }
                }
                if (indexInteractive) {
                    if (indexProperties.replicas() != 2) {
                        setNumReplicasForIndex(index.getName(), 2, numReplicaChanges);
                    }
                } else {
                    if (indexProperties.replicas() != 1) {
                        setNumReplicasForIndex(index.getName(), 1, numReplicaChanges);
                    }
                }
            }
        } else {
            // search power should be between 100 and 250 here
            long allIndicesInteractiveSize = 0;
            List<IndexReplicationRanker.IndexRankingProperties> rankingProperties = new ArrayList<>();
            for (Map.Entry<Index, IndexProperties> entry : indicesMap.entrySet()) {
                Index index = entry.getKey();
                IndexProperties indexProperties = entry.getValue();
                long totalIndexInteractiveSize = 0;
                for (int i = 0; i < indexProperties.shards(); i++) {
                    SearchMetricsService.ShardMetrics shardMetrics = shardMetricsMap.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // continue with the next shard, this one might be removed or have 0 replicas but it
                        // can also be from a tiny index with skewed document distribution and some shards
                        // not having any documents in them
                        continue;
                    }
                    totalIndexInteractiveSize += shardMetrics.shardSize.interactiveSizeInBytes();
                }
                rankingProperties.add(new IndexReplicationRanker.IndexRankingProperties(indexProperties, totalIndexInteractiveSize));
                allIndicesInteractiveSize += totalIndexInteractiveSize;
            }
            final long threshold = allIndicesInteractiveSize * (searchPowerMinSetting - SEARCH_POWER_MIN_NO_REPLICATION)
                / (SEARCH_POWER_MIN_FULL_REPLICATION - SEARCH_POWER_MIN_NO_REPLICATION);
            Set<String> twoReplicaEligibleIndices = getRankedIndicesBelowThreshold(rankingProperties, threshold);
            for (var rankedIndex : rankingProperties) {
                String indexName = rankedIndex.indexProperties().name();
                int replicas = rankedIndex.indexProperties().replicas();
                if (rankedIndex.interactiveSize() > 0 && twoReplicaEligibleIndices.contains(indexName)) {
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
        Map<Index, IndexProperties> indicesMap = this.getFilteredIndices();
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
            Settings settings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicasTarget).build();
            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indices.toArray(new String[0]));
            client.executeLocally(TransportUpdateSettingsAction.TYPE, request, new ActionListener<>() {
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
     * Schedule task that gets the last replicas update recommendations and performs a settings update.
     */
    void performReplicaUpdates() {
        if (running.compareAndSet(false, true)) {
            // if the feature is currently disabled, we only need to check if we need to clean something up and return
            try {
                if (checkDisabled()) return;
            } finally {
                this.running.compareAndSet(true, false);
            }
            LOGGER.debug("running replicas update task. SP_min: " + this.searchPowerMinSetting);
            if (searchPowerMinSetting < SEARCH_POWER_MIN_NO_REPLICATION) {
                // we can scale everything down immediately
                publishUpdateReplicaSetting(1, resetReplicasForAllIndices());
            } else {
                Map<Integer, Set<String>> numberOfReplicaChanges = getRecommendedReplicaChanges();

                // apply scaling up to two replica suggestions immediately
                Set<String> indicesToScaleUp = numberOfReplicaChanges.remove(2);
                if (indicesToScaleUp != null) {
                    publishUpdateReplicaSetting(2, indicesToScaleUp);
                }

                // Scale down decisions requires a certain number of repetitions to be considered stable.
                // We want to avoid flapping up/down scaling decisions because if we scale up again soon
                // the performance cost of this outweighs the cost of keeping two replicas around longer.
                // This is also necessary for SPmin >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} because
                // even in that case indices might enter and fall out of the interactive boosting window.

                Set<String> indicesToScaleDown = numberOfReplicaChanges.remove(1);
                if (indicesToScaleDown != null) {
                    if (ensureRunning() == false) {
                        // break out if some other thread canceled the job at this point.
                        return;
                    }
                    Set<String> scaleDownUpdatesToSend = new HashSet<>();
                    for (String index : indicesToScaleDown) {
                        AtomicInteger scaleDownRepetitions = scaleDownCounters.computeIfAbsent(index, k -> new AtomicInteger(0));
                        if (scaleDownRepetitions.incrementAndGet() >= scaledownRepetitionSetting) {
                            scaleDownUpdatesToSend.add(index);
                        }
                        publishUpdateReplicaSetting(1, scaleDownUpdatesToSend);
                    }
                }
                // We only need to keep counters for scaling down candidates that haven't been included in this round's
                // updates, e.g. because they haven't reached the number of repetitions needed for stabilization yet.
                // We can remove all counters that are not part of this update's indices to scale down.
                if (indicesToScaleDown == null) {
                    clearCounters();
                } else {
                    scaleDownCounters.entrySet().removeIf(e -> indicesToScaleDown.contains(e.getKey()) == false);
                }
                assert numberOfReplicaChanges.isEmpty() : "we should have processed all requested replica demand changes";
            }
            this.running.compareAndSet(true, false);
            LOGGER.debug("completed replicas update task");
        } else {
            LOGGER.debug("skip running replicas update task, there is one instance in progress already");
        }
    }

    private boolean checkDisabled() {
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
        }
    }

    private void clearCounters() {
        this.scaleDownCounters.clear();
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
