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

package org.elasticsearch.xpack.stateless.autoscaling.search;

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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyListener;
import org.elasticsearch.xpack.stateless.autoscaling.search.IndexReplicationRanker.IndexRankingProperties;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult;
import org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterExecutionListener.NoopExecutionListener;
import org.elasticsearch.xpack.stateless.autoscaling.search.SearchMetricsService.IndexProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicaRankingContext.DEFAULT_NUMBER_OF_REPLICAS;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler.EMPTY_RESULT;

/**
 * This service controls the 'number_of_replicas' setting for all project indices in the search tier.
 * It periodically runs on the master node with an interval defined by the
 * {@link org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_INTERVAL}
 * setting, which defaults to 5 minutes. On a high level it polls index statistics and properties from
 * {@link org.elasticsearch.xpack.stateless.autoscaling.search.SearchMetricsService}, then decides which
 * indices should get one or two search tier replica and finally publishes the necessary updates of the
 * 'number_of_replicas' index setting.
 *
 * <h2>ConfigurableSettings</h2>
 *
 * <ul>
 * <li>
 *     "serverless.autoscaling.replica_updater_sample_interval"
 *     ({@link org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_INTERVAL})
 *     <p>
 *     Setting controlling the frequency in which this service pulls for new replica update suggestions.
 *     Defaults to 5 minutes.
 * </li>
 * <li>
 *     "serverless.autoscaling.replica_updater_scaledown_repetitions"
 *     ({@link org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_SCALEDOWN_REPETITIONS})
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
 * {@link org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService#REPLICA_UPDATER_SCALEDOWN_REPETITIONS}.
 * (defaults to 6).
 * <p>
 * This stabilization period prevents premature removal of replicas when an index is frequently entering and leaving
 * the group of indices eligible for a second replica, i.e. because data falling out of the boost window or global changes
 * in "total interactive size" that can lead to an oscillating size threshold. This consequently can lead to indices that are
 * right on the edge of getting promoted to be scaled too frequently, which in turn involves avoidable cost search cache
 * population etc...
 */
public class ReplicasUpdaterService extends AbstractLifecycleComponent implements LocalNodeMasterListener, DesiredTopologyListener {
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
    private ReplicasScalerCacheBudget replicasScalerCacheBudget;

    // package-private for testing
    final ReplicasScaleDownState replicasScaleDownState;
    private final ThreadPool threadPool;
    private final ReplicasUpdaterExecutionListener listener;
    private final DesiredTopologyContext desiredTopologyContext;

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

    // For metrics
    private volatile long totalReplicas;
    private volatile long totalDesiredReplicas;
    private volatile long indicesBlockedFromScaleUp;
    private final LongHistogram replicaIncreaseHistogram;
    private final LongHistogram replicaDecreaseHistogram;

    public ReplicasUpdaterService(
        ThreadPool threadPool,
        ClusterService clusterService,
        NodeClient client,
        SearchMetricsService searchMetricsService,
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler,
        DesiredTopologyContext desiredTopologyContext,
        MeterRegistry meterRegistry
    ) {
        this(
            threadPool,
            clusterService,
            client,
            searchMetricsService,
            replicasLoadBalancingScaler,
            desiredTopologyContext,
            new NoopExecutionListener(),
            meterRegistry
        );
    }

    public ReplicasUpdaterService(
        ThreadPool threadPool,
        ClusterService clusterService,
        NodeClient client,
        SearchMetricsService searchMetricsService,
        ReplicasLoadBalancingScaler replicasLoadBalancingScaler,
        DesiredTopologyContext desiredTopologyContext,
        ReplicasUpdaterExecutionListener listener,
        MeterRegistry meterRegistry
    ) {
        this.threadPool = threadPool;
        this.client = client;
        this.searchMetricsService = searchMetricsService;
        this.clusterService = clusterService;
        this.replicasLoadBalancingScaler = replicasLoadBalancingScaler;
        replicasLoadBalancingScaler.init();
        this.replicasScaleDownState = new ReplicasScaleDownState();
        this.desiredTopologyContext = desiredTopologyContext;
        this.listener = listener;
        meterRegistry.registerLongGauge(
            "es.autoscaling.search.total_replicas.current",
            "The current total number of replica shards (over all indices in a project) before a replicas updater run.",
            "replicas",
            () -> new LongWithAttributes(this.totalReplicas)
        );
        meterRegistry.registerLongGauge(
            "es.autoscaling.search.total_desired_replicas.current",
            "The desired total number of replica shards (over all indices in a project) after a replicas updater run.",
            "replicas",
            () -> new LongWithAttributes(this.totalDesiredReplicas)
        );
        meterRegistry.registerLongGauge(
            "es.autoscaling.search.indices_blocked_scaling_up.current",
            "The number of indices blocked from scaling up replicas in the latest replicas updater run "
                + "(either due to in-progress search node shutdowns or cache budget exceeded).",
            "indices",
            () -> new LongWithAttributes(this.indicesBlockedFromScaleUp)
        );
        this.replicaIncreaseHistogram = meterRegistry.registerLongHistogram(
            "es.autoscaling.search.replica_increase.histogram",
            "Histogram of replica count increases per index",
            "replicas"
        );
        this.replicaDecreaseHistogram = meterRegistry.registerLongHistogram(
            "es.autoscaling.search.replica_decrease.histogram",
            "Histogram of replica count decreases per index",
            "replicas"
        );
    }

    public void init() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_FOR_INSTANT_FAILOVER, this::updateEnableReplicasForInstantFailover);
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_LOAD_BALANCING, this::updatedEnableReplicasLoadBalancing);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_INTERVAL, this::setInterval);
        clusterSettings.initializeAndWatch(REPLICA_UPDATER_SCALEDOWN_REPETITIONS, this::setScaledownRepetitionSetting);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
        clusterSettings.initializeAndWatch(AUTO_EXPAND_REPLICA_INDICES, this::updateAutoExpandReplicaIndices);
        desiredTopologyContext.addListener(this);
        replicasScalerCacheBudget = new ReplicasScalerCacheBudget(
            clusterService.getSettings(),
            desiredTopologyContext,
            replicasScaleDownState,
            scaledownRepetitionSetting
        );
        clusterSettings.initializeAndWatch(ReplicasScalerCacheBudget.REPLICA_CACHE_BUDGET_RATIO, ratio -> {
            replicasScalerCacheBudget.setCacheBudgetRatio(ratio);
            if (job != null) {
                performReplicaUpdates(false);
            }
        });
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
            pendingScaleDownAfterDisabling = false;
            LOGGER.info("enabling replicas for instant failover");
        } else {
            LOGGER.info("disabling replicas for instant failover");
            pendingScaleDownAfterDisabling = true;
        }
    }

    void updatedEnableReplicasLoadBalancing(Boolean newValue) {
        replicasLoadBalancingScaler.updateEnableReplicasForLoadBalancing(newValue);
        if (newValue) {
            pendingScaleDownAfterDisabling = false;
            LOGGER.info("enabling replicas for load balancing");
        } else {
            LOGGER.info("disabling replicas for load balancing");
            pendingScaleDownAfterDisabling = true;
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
     * This method calculates the desired indices replicas state for instant failover
     * setting based on the current Search Power (SP) setting.
     * For SP gte {@link #SEARCH_POWER_MIN_FULL_REPLICATION} all interactive indices get two replicas.
     * For settings between {@link #SEARCH_POWER_MIN_NO_REPLICATION} and {@link #SEARCH_POWER_MIN_FULL_REPLICATION} values, we rank indices
     * and give part of them two replicas.
     * For SP less than {@link #SEARCH_POWER_MIN_NO_REPLICATION} every index gets only one replica.
     */
    static Map<String, Integer> getRecommendedReplicasState(ReplicaRankingContext rankingContext) {
        Map<String, Integer> desiredReplicasState = Maps.newHashMapWithExpectedSize(rankingContext.indices().size());

        LOGGER.debug("Calculating index replica recommendations for " + rankingContext.indices());
        if (rankingContext.getSearchPowerMin() >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            for (IndexRankingProperties properties : rankingContext.properties()) {
                String indexName = properties.indexProperties().name();
                if (properties.isInteractive()) {
                    desiredReplicasState.put(indexName, 2);
                } else {
                    desiredReplicasState.put(indexName, 1);
                }
            }
        } else if (rankingContext.getSearchPowerMin() > SEARCH_POWER_MIN_NO_REPLICATION) {
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
                if (twoReplicaEligibleIndices.contains(indexName)) {
                    assert rankedIndex.isInteractive() : "only interactive indices should get additional copies";
                    desiredReplicasState.put(indexName, 2);
                } else {
                    desiredReplicasState.put(indexName, 1);
                }
            }
        } else {
            // search power is less than or equal to 100, all indices should have 1 replica
            desiredReplicasState.putAll(rankingContext.indices().stream().collect(toMap(indexName -> indexName, indexName -> 1)));
        }
        return desiredReplicasState;
    }

    private void publishUpdateReplicaSetting(int numReplicasTarget, Set<String> indices, ReplicaRankingContext rankingContext) {
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
                    replicasScaleDownState.clearStateForIndices(indices);
                    LOGGER.debug("Updated replicas for " + indices + " to " + numReplicasTarget);
                    // Record histogram metrics after successful update
                    for (String indexName : indices) {
                        IndexRankingProperties props = rankingContext.rankingProperties(indexName);
                        if (props != null) {
                            int currentReplicas = props.indexProperties().replicas();
                            int delta = numReplicasTarget - currentReplicas;
                            if (delta > 0) {
                                replicaIncreaseHistogram.record(delta);
                            } else if (delta < 0) {
                                replicaDecreaseHistogram.record(-delta);
                            }
                        }
                    }
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
                listener.onRunStart(shouldScaleDownImmediately, onlyScaleDownToTopologyBounds);
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
        if (checkDisabledAndNeedsScaledown(searchMetricsService::createRankingContext)) {
            return;
        }

        final ReplicaRankingContext rankingContext = searchMetricsService.createRankingContext();
        this.totalReplicas = rankingContext.getTotalReplicas();
        LOGGER.debug("Ranking context: " + rankingContext);
        replicasLoadBalancingScaler.getRecommendedReplicas(
            clusterService.state(),
            rankingContext,
            desiredTopologyContext.getDesiredClusterTopology(),
            onlyScaleDownToTopologyBounds,
            new ActionListener<>() {
                @Override
                public void onResponse(ReplicasLoadBalancingResult replicasLoadBalancingResult) {
                    Map<String, Integer> instantFailoverReplicaChanges = enableReplicasForInstantFailover
                        ? getRecommendedReplicasState(rankingContext)
                        : Map.of();
                    computeAndApplyReplicaChanges(
                        replicasScalerCacheBudget.applyCacheBudgetConstraint(
                            rankingContext,
                            instantFailoverReplicaChanges,
                            replicasLoadBalancingResult,
                            immediateScaleDown
                        ),
                        instantFailoverReplicaChanges,
                        rankingContext,
                        immediateScaleDown,
                        onlyScaleDownToTopologyBounds
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    // famous last words, this should never happen. well, falling back to executing replicas for instant failover if it
                    // does indeed happen
                    LOGGER.error("Error getting replica changes from load balancing scaler. Executing replicas for instant failover.", e);
                    computeAndApplyReplicaChanges(
                        EMPTY_RESULT,
                        enableReplicasForInstantFailover ? getRecommendedReplicasState(rankingContext) : Map.of(),
                        rankingContext,
                        immediateScaleDown,
                        onlyScaleDownToTopologyBounds
                    );
                }
            }
        );
    }

    /**
     * Processes replica changes according to the load balancing scaler and instant failover recommendations
     * (it also handles auto-expand indices).
     * It applies both scale-up and scale-down operations.
     * Scale-downs are delayed unless immediate scale-down is requested (via boolean parameter) or topology bounds
     * require it (via @link{{@link ReplicasLoadBalancingResult#immediateReplicaScaleDown()}).
     */
    private void computeAndApplyReplicaChanges(
        ReplicasLoadBalancingResult replicasLoadBalancingResult,
        Map<String, Integer> instantFailoverReplicaChanges,
        ReplicaRankingContext rankingContext,
        boolean requestedImmediateScaleDown,
        boolean onlyScaleDownToTopologyBounds
    ) {
        this.indicesBlockedFromScaleUp = replicasLoadBalancingResult.indicesBlockedFromScaleUp();
        int indicesScaledDown = 0;
        int indicesScaledUp = 0;

        int numSearchNodes = desiredTopologyContext.getDesiredClusterTopology() == null
            ? ReplicasUpdaterService.this.numSearchNodes
            : desiredTopologyContext.getDesiredClusterTopology().getSearch().getReplicas();

        Map<String, Integer> loadBalancingReplicaChanges = replicasLoadBalancingResult.desiredReplicasPerIndex();
        Map<Integer, Set<String>> immediateScaleDownChanges = new HashMap<>(
            replicasLoadBalancingResult.immediateReplicaScaleDown().isEmpty() ? 0 : 5, // picking a size likely to be close
                                                                                       // to how many keys we'll have (note
                                                                                       // it's the target replica count on
                                                                                       // immediate scale downs)
            1.0f
        );
        // transform the immediateReplicaScaleDown from <String, Integer> to <Integer, Set<String>> for easier processing of
        // replica changes in batches
        replicasLoadBalancingResult.immediateReplicaScaleDown()
            .forEach((k, v) -> immediateScaleDownChanges.computeIfAbsent(v, x -> new HashSet<>()).add(k));

        // we REALLY need to scale down the replicas for these indices so send it right away
        for (Map.Entry<Integer, Set<String>> immediateScaleDown : immediateScaleDownChanges.entrySet()) {
            publishUpdateReplicaSetting(immediateScaleDown.getKey(), immediateScaleDown.getValue(), rankingContext);
            indicesScaledDown += immediateScaleDown.getValue().size();
        }

        Map<Integer, Set<String>> indicesToScaleUp = new HashMap<>(rankingContext.indices().size());
        Map<Integer, Set<String>> indicesToScaleDown = new HashMap<>(rankingContext.indices().size());
        // This holds all changes on indices marked for "auto-expand." These must be scaled to the current number of search
        // nodes.
        Set<String> autoExpandIndices = new HashSet<>();
        int totalDesiredReplicas = 0;
        for (IndexRankingProperties property : rankingContext.properties()) {
            String indexName = property.indexProperties().name();
            int currentReplicas = property.indexProperties().replicas();
            // Handle all auto-expand functionality separately.
            if (autoExpandReplicaIndices.contains(indexName)) {
                // Auto-expand should only scale up for "full replication" values of SPmin.
                // However, no matter the value of SPmin, we should always scale down auto-expanded indices so that replicas
                // never exceed the number of search nodes.
                if ((rankingContext.getSearchPowerMin() >= SEARCH_POWER_MIN_FULL_REPLICATION
                    && numSearchNodes > 2
                    && currentReplicas < numSearchNodes) || currentReplicas > numSearchNodes) {
                    totalDesiredReplicas += numSearchNodes;
                    autoExpandIndices.add(indexName);
                    continue;
                }
            }
            int recommendedReplicas = Math.max(
                instantFailoverReplicaChanges.getOrDefault(indexName, DEFAULT_NUMBER_OF_REPLICAS),
                loadBalancingReplicaChanges.getOrDefault(indexName, DEFAULT_NUMBER_OF_REPLICAS)
            );
            totalDesiredReplicas += recommendedReplicas;
            if (recommendedReplicas > currentReplicas) {
                indicesToScaleUp.computeIfAbsent(recommendedReplicas, k -> new HashSet<>()).add(indexName);
            } else if (recommendedReplicas < currentReplicas) {
                indicesToScaleDown.computeIfAbsent(recommendedReplicas, k -> new HashSet<>()).add(indexName);
            }
        }
        this.totalDesiredReplicas = totalDesiredReplicas;

        // apply scaling up to two replica suggestions immediately
        for (Map.Entry<Integer, Set<String>> scaleUpEntry : indicesToScaleUp.entrySet()) {
            Integer targetReplicasCount = scaleUpEntry.getKey();
            Set<String> indices = scaleUpEntry.getValue();
            publishUpdateReplicaSetting(targetReplicasCount, indices, rankingContext);
            indicesScaledUp += indices.size();
        }

        // apply auto-expand changes immediately
        publishUpdateReplicaSetting(numSearchNodes, autoExpandIndices, rankingContext);

        // Scale down decisions require a certain number of repetitions to be considered stable.
        // We want to avoid flapping up/down scaling decisions because if we scale up again soon
        // the performance cost of this outweighs the cost of keeping two replicas around longer.
        // This is also necessary for SPmin >= {@link #SEARCH_POWER_MIN_FULL_REPLICATION} because
        // even in that case indices might enter and fall out of the interactive boosting window.
        Set<String> indicesTrackedForScaleDown = new HashSet<>();
        // scaleDownUpdatesToSend holds replica updates that will eventually be published. We batch
        // these by replica count in order to publish one update per replica count. indicesToScaleDown
        // also holds replica counts, but we may end up publishing a larger number due to the maximum
        // recommended replica logic in ReplicasScaleDownState.
        Map<Integer, Set<String>> scaleDownUpdatesToSend = new HashMap<>(rankingContext.indices().size());
        for (Map.Entry<Integer, Set<String>> scaleDownEntry : indicesToScaleDown.entrySet()) {
            if (ensureRunning() == false) {
                // break out if some other thread canceled the job at this point.
                return;
            }
            Integer targetReplicasCount = scaleDownEntry.getKey();
            Set<String> indices = scaleDownEntry.getValue();
            if (requestedImmediateScaleDown) {
                LOGGER.info("Immediately scaling down replicas to {} for indices {}", targetReplicasCount, indices);
                publishUpdateReplicaSetting(targetReplicasCount, indices, rankingContext);
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
                publishUpdateReplicaSetting(numReplicas, indices, rankingContext);
                indicesScaledDown += indices.size();
            }
            ReplicasUpdaterService.this.replicasScaleDownState.clearStateExceptForIndices(indicesTrackedForScaleDown);
        }
        LOGGER.debug("RIF: {}, RLB: {}", instantFailoverReplicaChanges, replicasLoadBalancingResult);
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
                + ", onlyScaleDownToTopologyBounds: "
                + onlyScaleDownToTopologyBounds
        );
    }

    /**
     * Update each index's {@link #replicasScaleDownState} and decide whether to stage a scale-down for publishing.
     * @param scaleDownUpdatesToSend A map to stage scale-downs that will be published
     * @param scaleDownIndices The indices being considered
     * @param targetReplicasCount The number of replicas recommended for the indices being considered
     */
    void populateScaleDownUpdates(Map<Integer, Set<String>> scaleDownUpdatesToSend, Set<String> scaleDownIndices, int targetReplicasCount) {
        for (String index : scaleDownIndices) {
            ReplicasScaleDownState.PerIndexState updatedState = replicasScaleDownState.updateMaxReplicasRecommended(
                index,
                targetReplicasCount
            );
            if (updatedState.signalCount() >= scaledownRepetitionSetting) {
                scaleDownUpdatesToSend.computeIfAbsent(updatedState.maxReplicasRecommended(), k -> new HashSet<>()).add(index);
            }
        }
    }

    private boolean checkDisabledAndNeedsScaledown(Supplier<ReplicaRankingContext> rankingContextSupplier) {
        // all replicas changes scalers are disabled, scale everything down to 1 replica
        boolean replicasUpdaterScalersDisabled = this.enableReplicasForInstantFailover == false
            && replicasLoadBalancingScaler.isEnabled() == false;
        if (replicasUpdaterScalersDisabled) {
            // we might need to scale down indices if the replica feature was disabled
            if (this.pendingScaleDownAfterDisabling) {
                LOGGER.debug(
                    "both instant failover and load balancing replica scalers are disabled, scaling all indices down to 1 replica"
                );
                Set<String> indicesToScaleDown = new HashSet<>();
                Map<Index, IndexProperties> indicesMap = this.searchMetricsService.getIndices();
                for (Map.Entry<Index, IndexProperties> entry : indicesMap.entrySet()) {
                    Index index = entry.getKey();
                    IndexProperties settings = entry.getValue();
                    if (settings.replicas() != 1) {
                        indicesToScaleDown.add(index.getName());
                    }
                }

                ReplicaRankingContext rankingContext = rankingContextSupplier.get();
                this.totalReplicas = rankingContext.getTotalReplicas();
                this.totalDesiredReplicas = indicesMap.size();
                publishUpdateReplicaSetting(1, indicesToScaleDown, rankingContext);
                this.pendingScaleDownAfterDisabling = false;
                this.replicasScaleDownState.clearState();
            }
        }
        return replicasUpdaterScalersDisabled;
    }

    /**
     * make sure the job isn't cancelled yet, if so we also clear internal state.
     * Returns `true` if we are still running the job.
     */
    private boolean ensureRunning() {
        if (job == null || job.isCancelled()) {
            this.replicasScaleDownState.clearState();
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
            this.replicasScaleDownState.clearState();
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
        this.replicasScaleDownState.clearState();
        scheduleTask();
    }

    @Override
    public void onDesiredTopologyAvailable(DesiredClusterTopology topology) {
        // make sure the index replicas in the project stay within the desired topology bounds
        performReplicaUpdates(false, true);
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
            if (event.localNodeMaster() == false) {
                return;
            }
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
