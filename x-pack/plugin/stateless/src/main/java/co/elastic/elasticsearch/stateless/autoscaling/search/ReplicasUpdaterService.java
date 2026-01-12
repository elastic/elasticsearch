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
import co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterExecutionListener.NoopExecutionListener;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;

/**
 * This service controls the 'number_of_replicas' setting for all project indices in the search tier.
 * It periodically runs on the master node with an interval defined by the
 * {@link co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_INTERVAL}
 * setting, which defaults to 5 minutes. On a high level it polls index statistics and properties from
 * {@link co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService}, then decides which
 * indices should get one or two search tier replica and finally publishes the necessary updates of the
 * 'number_of_replicas' index setting.
 *
 * <h2>ConfigurableSettings</h2>
 *
 * <ul>
 * <li>
 *     "serverless.autoscaling.replica_updater_sample_interval"
 *     ({@link co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_INTERVAL})
 *     <p>
 *     Setting controlling the frequency in which this service pulls for new replica update suggestions.
 *     Defaults to 5 minutes.
 * </li>
 * <li>
 *     "serverless.autoscaling.replica_updater_scaledown_repetitions"
 *     ({@link co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_SCALEDOWN_REPETITIONS})
 *     <p>
 *     Controls how many repeated scale down signals we need to receive in order to perform a scale down to 1
 *     replica. Defaults to 6.
 * </li>
 * <li>
 *     "serverless.search.enable_replicas_for_instant_failover"
 *     (({@link co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings#ENABLE_REPLICAS_FOR_INSTANT_FAILOVER}))
 *     <p>
 *     If {@code true}, auto-adjustment of replicas is enabled.
 * </li>
 * </ul>
 *
 * <h2>Automatic replica adjustment</h2>
 *
 * By default, every index already has one replica in the search tier. To speed up failover for important indices, the
 * ReplicasUpdaterService can increase the `number_of_replica` setting of indices to 2. This decision is controlled by
 * the projects current {@link co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings#SEARCH_POWER_MIN_SETTING}
 * (short <b>SPmin</b>).
 * <p>
 * Another important factor is an index <b>interactive data size</b>, which is the joint size of all documents with
 * a {@code @timestamp} field value that lies in the projects boost window.
 * ({@link co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings#BOOST_WINDOW_SETTING}).
 * Regular indices that don't have an {@code @timestamp} field are considered to be completely interactive.
 * The <b>total interactive size</b> is the joint interactive data size of all indices in a project.
 * <p>
 * There are three different scenarios:
 *
 * <ul>
 *     <li>
 *         {@code SPmin <= 100} : all indices only receive one replica.
 *     </li>
 *     <li>
 *         {@code 100 < SPmin < 250} : indices are ranked based on some "importance" heuristics and only some of them
 *         get 2 replicas.
 *     </li>
 *     <li>
 *         {@code SPmin >= 250} : indices with interactive data receive two replica. Non-interactive indices remain at one replica.
 *     </li>
 * </ul>
 *
 * When SPmin is between 100 and 250, we determine a memory budget that we are willing to spend on a second set of
 * replicas for interactive indices. The formula for the threshold is:
 * <p>
 * threshold =  ((SPmin - 100)/(250 - 150)) * total_interactive_size
 * </p>
 * which represents the portion of the "total interactive size" of the project that we are willing to spent on
 * additional replicas. This is proportional to the current SPmin settings value between 100 and 250, e.g.
 * with a setting of SPmin = 175 we are allowing for 1/2 * total_interactive_size to be spent on additional replica.
 * <p>
 * In order to determine which indices are eligible for a second replica we rank them according to the following rules:
 * <ul>
 *     <li>system indices receive the highest rank, sorted by size in descending order.</li>
 *     <li>regular indices are ranked after that, ties are broken by size (highest first)</li>
 *     <li>data streams backing indices are ranked last. Current write indices are ranked before less recent backing
 *     indices. Again, ties are broken by size (highest first)</li>
 * </ul>
 * Using this index ordering, the ReplicasUpdaterService proposes to add replicas to indices whose cumulative interactive
 * data size doesn't exceed the threshold, going from top to bottom. All remaining indices are receiving only one replica.
 *
 * <h2>Delayed replica scale-down</h2>
 *
 * Adding additional replicas to indices is performed immediately with the next run of the service task.
 * Also, we scale up or down immediately on changes to SPmin. For all other scale-down cases we delay publishing the
 * settings change in order to stabilize the decision until we have seen a repeated scale-down signal
 * for more repetitions than configured by
 * {@link co.elastic.elasticsearch.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_SCALEDOWN_REPETITIONS}.
 * (defaults to 6).
 * <p>
 * This stabilization period prevents premature removal of replicas when an index is frequently entering and leaving
 * the group of indices eligible for a second replica, i.e. because data falling out of the boost window or global changes
 * in "total interactive size" that can lead to an oscillating size threshold. This consequently can lead to indices that are
 * right on the edge of getting promoted to be scaled too frequently, which in turn involves avoidable cost search cache
 * population etc...
 */
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
     * This setting can be used to work around the lack of replicas for load balancing.
     * You can either get 1 or 2 replica shards based on SPmin, but there are situations where high throughput is a requirement,
     * which requires additional replicas. This setting allows us to configure via override a set of indices that needs to have
     * as many replicas as the number of search nodes available. Note that there is no validation of the disk size occupied by
     * such indices, which is why this setting should be used carefully, as a short term mitigation.
     * This setting is to be considered temporary, and will be removed once replicas for load balancing is implemented, which will
     * increase and decrease number of replicas for indices based on measured search load per index.
     */
    public static final Setting<List<String>> AUTO_EXPAND_REPLICA_INDICES = Setting.stringListSetting(
        "serverless.autoscaling.auto_expand_replica_indices",
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
    private final ReplicasLoadBalancingScaler replicasLoadBalancingScaler;

    // package-private for testing
    final ScaleDownState scaleDownState;
    private final ThreadPool threadPool;
    private final ReplicasUpdaterExecutionListener listener;

    // State machine to manage the scheduling of the job (we want to prevent multiple runs in parallel, and
    // to have a way to run a potential pending track that was scheduled outside of the scheduled loop, e.g. on master
    // failover or setting change)
    // The state will change from RUNNING to RUNNING_WITH_PENDING in case a new run is requested while a run is already in progress. The
    // loop will then, immediately, execute another run after finishing the current one.
    // If multiple concurrent trigger events occur while a run is in progress, they will be coalesced into a single pending run.
    // The IDLE state means no run is in progress nor pending.
    public enum State {
        IDLE,
        RUNNING,
        RUNNING_WITH_PENDING
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private final AtomicBoolean pendingImmediateScaleDown = new AtomicBoolean(false);
    // The latestRequestedTopologyOnly field tracks whether the most recent pending request should only perform a topology check (a subset
    // of a full replica update). It's used in performReplicaUpdates to coordinate between concurrent calls when multiple requests arrive
    // while a run is in progress. This stores the latest onlyScaleDownToTopologyBounds value, which is then used when the pending run
    // executes. Unlike pendingImmediateScaleDown, losing a true value is acceptable since onlyScaleDownToTopologyBounds=false encompasses
    // onlyScaleDownToTopologyBounds=true.
    private final AtomicBoolean latestRequestedTopologyOnly = new AtomicBoolean(false);

    private volatile boolean pendingScaleDownAfterDisabling = false;
    private volatile boolean enableReplicasForInstantFailover;
    private volatile Cancellable job;
    private volatile TimeValue updateInterval;
    private volatile Integer scaledownRepetitionSetting;
    private volatile Set<String> autoExpandReplicaIndices;
    private volatile int searchPowerMinSetting;
    private volatile int numSearchNodes;

    /**
     * Tracks scale-down recommendations per index, including the number of signals received and the
     * highest replica count recommended across all signals.
     * <p>Thread-safety: clear operations may run concurrently due to the atomic {@link #updateMaxReplicasRecommended}.
     */
    static class ScaleDownState {

        private final ConcurrentHashMap<String, PerIndexState> scaleDownStateByIndex;

        ScaleDownState() {
            scaleDownStateByIndex = new ConcurrentHashMap<>();
        }

        /**
         * Clear state for every index.
         */
        void clearState() {
            scaleDownStateByIndex.clear();
        }

        /**
         * Clear state for the given indices.
         * @param indices the indices to clear.
         */
        void clearStateForIndices(Collection<String> indices) {
            if (indices != null && indices.isEmpty() == false) {
                scaleDownStateByIndex.entrySet().removeIf(e -> indices.contains(e.getKey()));
            }
        }

        /**
         * Clear state for all indices not given.
         * @param indices the indices to keep.
         */
        void clearStateExceptForIndices(Collection<String> indices) {
            if (indices == null || indices.isEmpty()) {
                scaleDownStateByIndex.clear();
            } else {
                scaleDownStateByIndex.entrySet().removeIf(e -> indices.contains(e.getKey()) == false);
            }
        }

        /**
         * Update the max number of replicas to scale down to for the given index. Also increments the
         * signal count for the given index.
         * @param index the index
         * @param replicasRecommended the recommended number of replicas
         * @return the updated state for the given index
         */
        PerIndexState updateMaxReplicasRecommended(String index, int replicasRecommended) {
            return scaleDownStateByIndex.compute(index, (k, existing) -> {
                if (existing == null) {
                    return new PerIndexState(1, replicasRecommended);
                }
                return new PerIndexState(existing.signalCount() + 1, Math.max(existing.maxReplicasRecommended(), replicasRecommended));
            });
        }

        boolean isEmpty() {
            return scaleDownStateByIndex.isEmpty();
        }

        record PerIndexState(int signalCount, int maxReplicasRecommended) {}
    }

    public ReplicasUpdaterService(
        ThreadPool threadPool,
        ClusterService clusterService,
        NodeClient client,
        SearchMetricsService searchMetricsService
    ) {
        this(threadPool, clusterService, client, searchMetricsService, new NoopExecutionListener());
    }

    @SuppressWarnings("this-escape")
    public ReplicasUpdaterService(
        ThreadPool threadPool,
        ClusterService clusterService,
        NodeClient client,
        SearchMetricsService searchMetricsService,
        ReplicasUpdaterExecutionListener listener
    ) {
        this.threadPool = threadPool;
        this.client = client;
        this.searchMetricsService = searchMetricsService;
        this.clusterService = clusterService;
        this.replicasLoadBalancingScaler = new ReplicasLoadBalancingScaler(clusterService, client);
        replicasLoadBalancingScaler.init();
        this.scaleDownState = new ScaleDownState();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_FOR_INSTANT_FAILOVER, this::updateEnableReplicasForInstantFailover);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_INTERVAL, this::setInterval);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_SCALEDOWN_REPETITIONS, this::setScaledownRepetitionSetting);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
        clusterSettings.initializeAndWatch(AUTO_EXPAND_REPLICA_INDICES, this::updateAutoExpandReplicaIndices);
        this.listener = listener;
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
            this.scaleDownState.clearState();
        }
    }

    void updateSearchPowerMin(Integer spMin) {
        this.searchPowerMinSetting = spMin;
        if (enableReplicasForInstantFailover && job != null) {
            // Note: this won't do anything if the job is already running.
            // The outcome may or may not take into account the updated SPmin.
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
     * For SP >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} all interactive indices get two replicas.
     * For settings between those values, we rank indices and give part of them two replicas.
     */
    static Map<String, Integer> getRecommendedReplicaChanges(ReplicaRankingContext rankingContext) {
        assert rankingContext.getSearchPowerMin() > 100 : "we should not have to call this method for SP <= 100";
        Map<String, Integer> numReplicaChanges = new HashMap<>(rankingContext.indices().size(), 1);

        LOGGER.debug("Calculating index replica recommendations for " + rankingContext.indices());
        if (rankingContext.getSearchPowerMin() >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            for (IndexRankingProperties properties : rankingContext.properties()) {
                String indexName = properties.indexProperties().name();
                int replicas = properties.indexProperties().replicas();
                if (properties.isInteractive()) {
                    if (replicas != 2) {
                        numReplicaChanges.put(indexName, 2);
                    }
                } else if (replicas != 1) {
                    numReplicaChanges.put(indexName, 1);
                }
            }
        } else {
            // search power should be between 100 and 250 here
            var rankingResult = getRankedIndicesBelowThreshold(rankingContext.properties(), rankingContext.getThreshold());
            LOGGER.debug(
                "last two replica eligible index: "
                    + rankingResult.lastTwoReplicaIndex()
                    + ", next candidate index: "
                    + rankingResult.firstOneReplicaIndex()
            );
            Set<String> twoReplicaEligibleIndices = rankingResult.twoReplicaEligableIndices();
            for (var rankedIndex : rankingContext.properties()) {
                String indexName = rankedIndex.indexProperties().name();
                int replicas = rankedIndex.indexProperties().replicas();
                if (twoReplicaEligibleIndices.contains(indexName)) {
                    assert rankedIndex.isInteractive() : "only interactive indices should get additional copies";
                    if (replicas != 2) {
                        numReplicaChanges.put(indexName, 2);
                    }
                } else {
                    if (replicas != 1) {
                        numReplicaChanges.put(indexName, 1);
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
                    scaleDownState.clearStateForIndices(indices);
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
     * Requests a run to get recommendations and potentially update the number of replicas (over all indices).
     * If a run is already in progress, enqueues a subsequent run via a {@link #state} change.
     */
    void performReplicaUpdates() {
        performReplicaUpdates(false);
    }

    /**
     * Requests a run to get recommendations and potentially update the number of replicas (over all indices).
     * If a run is already in progress, enqueues a subsequent run via a {@link #state} change.
     * @param immediateScaleDown whether the next run must immediately scale down in the case of a scale down
     *                           recommendation, rather than waiting for {@link #scaledownRepetitionSetting}
     *                           repeated signals.
     */
    // visible for testing
    void performReplicaUpdates(boolean immediateScaleDown) {
        performReplicaUpdates(immediateScaleDown, false, 0);
    }

    // visible for testing
    void performReplicaUpdates(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds) {
        performReplicaUpdates(immediateScaleDown, onlyScaleDownToTopologyBounds, 0);
    }

    private void performReplicaUpdates(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds, int depth) {
        if (immediateScaleDown) {
            pendingImmediateScaleDown.set(true);
        }
        // record whether this run should only do a topology check so we don't miss the intent of the run
        // note that we're handling this a bit different to immediateScaleDown as for immediate scale down we don't want to lose a seen
        // value of true (i.e. we must immediately scale down) whilst for onlyScaleDownToTopologyBounds we can just use the latest seen
        // value when coming back through the recursive call as onlyScaleDownToTopologyBounds=true is a subset of
        // onlyScaleDownToTopologyBounds=false so it's ok to lose such a request if we have 3 chained calls with
        // onlyScaleDownToTopologyBounds=true followed by onlyScaleDownToTopologyBounds=false
        latestRequestedTopologyOnly.set(onlyScaleDownToTopologyBounds);
        if (depth > 3) {
            // quite unlucky to get a few chained pending runs while we were already running, so let's skip chaining the runs and
            // at this point allow the run to go on the scheduled loop
            LOGGER.warn("postponing performReplicaUpdates after {} recursive calls until the natural scheduled loop", depth);
            return;
        }

        // make sure we record a pending run if the loop is currently running, or otherwise that we
        // "claim" this run by changing status to RUNNING
        State previousState = state.getAndUpdate(s -> s == State.IDLE ? State.RUNNING : State.RUNNING_WITH_PENDING);

        if (previousState == State.IDLE) {
            try {
                // set the pending scale down flag to false as we'll do the immediate scale down now (we're guaranteed to run) if there was
                // a pending request
                boolean shouldScaleDownImmediately = pendingImmediateScaleDown.getAndSet(false);
                listener.onRunStart(shouldScaleDownImmediately);
                run(shouldScaleDownImmediately, latestRequestedTopologyOnly.get());
                listener.onRunComplete();
            } finally {
                State stateAfterRun = state.getAndSet(State.IDLE);
                listener.onStateTransitionToIdle();
                if (stateAfterRun == State.RUNNING_WITH_PENDING) {
                    listener.onPendingRunExecution();
                    // don't lose the potential pending run request, but go back through the performReplicaUpdates entry point
                    // to make sure we didn't already re-start the loop after we set the state to IDLE
                    LOGGER.debug("triggering pending replicas update run");
                    // parameter doesn't matter here as we already captured the intent of a potential pending run
                    // and will check pendingImmediateScaleDown flag at the top
                    performReplicaUpdates(false, latestRequestedTopologyOnly.getAndSet(false), depth + 1);
                }
            }
        } else {
            LOGGER.debug("skip running replicas update task, there is one instance in progress already");
            listener.onSkipped();
        }
    }

    private void run(boolean immediateScaleDown, boolean onlyScaleDownToTopologyBounds) {
        if (checkDisabledAndNeedsScaledown()) {
            return;
        }

        int indicesScaledDown = 0;
        int indicesScaledUp = 0;
        ReplicaRankingContext rankingContext = searchMetricsService.createRankingContext();
        LOGGER.debug("Ranking context: " + rankingContext);
        if (rankingContext.getSearchPowerMin() <= SEARCH_POWER_MIN_NO_REPLICATION) {
            // we can scale everything down immediately
            Set<String> indicesToScaleDown = resetReplicasForAllIndices();
            publishUpdateReplicaSetting(1, indicesToScaleDown);
            indicesScaledDown = indicesToScaleDown.size();
        } else {
            int numSearchNodes = this.numSearchNodes;
            Map<String, Integer> recommendedReplicaChanges = getRecommendedReplicaChanges(rankingContext);

            Map<Integer, Set<String>> indicesToScaleUp = new HashMap<>(rankingContext.indices().size());
            Map<Integer, Set<String>> indicesToScaleDown = new HashMap<>(rankingContext.indices().size());
            // This holds all changes on indices marked for "auto-expand." These must be scaled to the current number of search nodes.
            Set<String> autoExpandIndices = new HashSet<>();
            for (IndexRankingProperties property : rankingContext.properties()) {
                String indexName = property.indexProperties().name();
                int currentReplicas = property.indexProperties().replicas();
                // Handle all auto-expand functionality separately.
                if (autoExpandReplicaIndices.contains(indexName)) {
                    // Auto-expand should only scale up for "full replication" values of SPmin.
                    // However, no matter the value of SPmin, we should always scale down auto-expanded indices so that replicas never
                    // exceed the number of search nodes.
                    if ((rankingContext.getSearchPowerMin() >= SEARCH_POWER_MIN_FULL_REPLICATION
                        && numSearchNodes > 2
                        && currentReplicas < numSearchNodes) || currentReplicas > numSearchNodes) {
                        autoExpandIndices.add(indexName);
                        continue;
                    }
                }
                Integer recommendedReplicas = recommendedReplicaChanges.get(indexName);
                if (recommendedReplicas != null) {
                    if (recommendedReplicas > currentReplicas) {
                        indicesToScaleUp.computeIfAbsent(recommendedReplicas, k -> new HashSet<>()).add(indexName);
                    } else if (recommendedReplicas < currentReplicas) {
                        indicesToScaleDown.computeIfAbsent(recommendedReplicas, k -> new HashSet<>()).add(indexName);
                    }
                }
            }

            // apply scaling up to two replica suggestions immediately
            for (Map.Entry<Integer, Set<String>> scaleUpEntry : indicesToScaleUp.entrySet()) {
                Integer targetReplicasCount = scaleUpEntry.getKey();
                Set<String> indices = scaleUpEntry.getValue();
                publishUpdateReplicaSetting(targetReplicasCount, indices);
                indicesScaledUp += indices.size();
            }

            // apply auto-expand changes immediately
            publishUpdateReplicaSetting(numSearchNodes, autoExpandIndices);

            // Scale down decisions require a certain number of repetitions to be considered stable.
            // We want to avoid flapping up/down scaling decisions because if we scale up again soon
            // the performance cost of this outweighs the cost of keeping two replicas around longer.
            // This is also necessary for SPmin >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} because
            // even in that case indices might enter and fall out of the interactive boosting window.
            Set<String> indicesTrackedForScaleDown = new HashSet<>();
            // scaleDownUpdatesToSend holds replica updates that will eventually be published. We batch
            // these by replica count in order to publish one update per replica count. indicesToScaleDown
            // also holds replica counts, but we may end up publishing a larger number due to the maximum
            // recommended replica logic in ScaleDownState.
            Map<Integer, Set<String>> scaleDownUpdatesToSend = new HashMap<>(rankingContext.indices().size());
            for (Map.Entry<Integer, Set<String>> scaleDownEntry : indicesToScaleDown.entrySet()) {
                if (ensureRunning() == false) {
                    // break out if some other thread canceled the job at this point.
                    return;
                }
                Integer targetReplicasCount = scaleDownEntry.getKey();
                Set<String> indices = scaleDownEntry.getValue();
                if (immediateScaleDown) {
                    publishUpdateReplicaSetting(targetReplicasCount, indices);
                    indicesScaledDown += indices.size();
                } else if (onlyScaleDownToTopologyBounds == false) {
                    indicesTrackedForScaleDown.addAll(indices);
                    populateScaleDownUpdates(scaleDownUpdatesToSend, indices, targetReplicasCount);
                }
            }
            // Topology checks might run often (e.g. when nodes leave the cluster) so we skip the
            // optional reduction of number of replicas when only topology check is requested because
            // if, say, 4 nodes leave a cluster that'll increment the scale down counters for some indices by
            // 4 only because we want to make sure we are within the topology bounds (we might end up reducing
            // the number of replicas after just 10 minutes, as we've incremented the counter 4 times, i.e. 20 minutes
            // due to topology checks).
            // Mandatory / immediate scale downs will run even during topology only checks.
            if (onlyScaleDownToTopologyBounds == false) {
                for (var numReplicas : scaleDownUpdatesToSend.keySet()) {
                    var indices = scaleDownUpdatesToSend.get(numReplicas);
                    publishUpdateReplicaSetting(numReplicas, indices);
                    indicesScaledDown += indices.size();
                }
                this.scaleDownState.clearStateExceptForIndices(indicesTrackedForScaleDown);
            }
        }

        LOGGER.info(
            "Finished replicas update task. Indices scaled up: "
                + indicesScaledUp
                + ", scaled down: "
                + indicesScaledDown
                + ". SPmin: "
                + rankingContext.getSearchPowerMin()
                + ", totalInteractiveSize: "
                + rankingContext.getAllIndicesInteractiveSize()
                + ", replica threshold: "
                + rankingContext.getThreshold()
        );
    }

    /**
     * Update each index's {@link #scaleDownState} and decide whether to stage a scale-down for publishing.
     * @param scaleDownUpdatesToSend A map to stage scale-downs that will be published
     * @param scaleDownIndices The indices being considered
     * @param targetReplicasCount The number of replicas recommended for the indices being considered
     */
    void populateScaleDownUpdates(Map<Integer, Set<String>> scaleDownUpdatesToSend, Set<String> scaleDownIndices, int targetReplicasCount) {
        for (String index : scaleDownIndices) {
            ScaleDownState.PerIndexState updatedState = scaleDownState.updateMaxReplicasRecommended(index, targetReplicasCount);
            if (updatedState.signalCount() >= scaledownRepetitionSetting) {
                scaleDownUpdatesToSend.computeIfAbsent(updatedState.maxReplicasRecommended(), k -> new HashSet<>()).add(index);
            }
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
            this.scaleDownState.clearState();
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

    private void unschedule(boolean clearState) {
        LOGGER.debug("unschedule replica updater task");
        if (job != null) {
            job.cancel();
            job = null;
        }
        if (clearState) {
            this.scaleDownState.clearState();
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
        // Ensure we don't start with any stale state
        this.scaleDownState.clearState();
        scheduleTask();
    }

    @Override
    public void offMaster() {
        unschedule(true);
    }

    void updateAutoExpandReplicaIndices(List<String> autoExpandReplicaIndices) {
        this.autoExpandReplicaIndices = new HashSet<>(autoExpandReplicaIndices);
        if (enableReplicasForInstantFailover && job != null) {
            if (autoExpandReplicaIndices.isEmpty() == false) {
                if (searchPowerMinSetting >= SEARCH_POWER_MIN_FULL_REPLICATION) {
                    // Note: this won't do anything if the job is already running.
                    // The outcome may or may not take into account the updated setting.
                    performReplicaUpdates();
                }
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        LocalNodeMasterListener.super.clusterChanged(event);
        if (event.nodesChanged()) {
            DiscoveryNodes nodes = event.state().getNodes();
            int numSearchNodes = (int) nodes.stream().filter(node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName())).count();
            boolean numSearchNodesChanged = this.numSearchNodes != numSearchNodes;
            this.numSearchNodes = numSearchNodes;
            if (event.nodesRemoved() && numSearchNodesChanged) {
                // as nodes come and go, just run topology checks on these events (includes auto-expand indices) so we
                // don't reduce the number of replicas too soon by decrementing the scale down counters
                performReplicaUpdates(false, true);
            } else if (enableReplicasForInstantFailover && job != null) {
                if (autoExpandReplicaIndices.isEmpty() == false) {
                    if (searchPowerMinSetting >= SEARCH_POWER_MIN_FULL_REPLICATION) {
                        if (numSearchNodesChanged) {
                            // Note: this won't do anything if the job is already running.
                            // The outcome may or may not take into account the updated number of search nodes.
                            performReplicaUpdates();
                        }
                    }
                }
            }
        }
    }

    // visible for testing
    State state() {
        return state.get();
    }
}
