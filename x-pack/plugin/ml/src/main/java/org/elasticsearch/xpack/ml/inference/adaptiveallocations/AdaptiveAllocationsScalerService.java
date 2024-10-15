/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Periodically schedules adaptive allocations scaling. This process consists
 * of calling the trained model stats API, processing the results, determining
 * whether scaling should be applied, and potentially calling the trained
 * model update API.
 */
public class AdaptiveAllocationsScalerService implements ClusterStateListener {

    record Stats(long successCount, long pendingCount, long failedCount, double inferenceTime) {

        long requestCount() {
            return successCount + pendingCount + failedCount;
        }

        double totalInferenceTime() {
            return successCount * inferenceTime;
        }

        Stats add(Stats value) {
            long newSuccessCount = successCount + value.successCount;
            long newPendingCount = pendingCount + value.pendingCount;
            long newFailedCount = failedCount + value.failedCount;
            double newInferenceTime = newSuccessCount > 0
                ? (totalInferenceTime() + value.totalInferenceTime()) / newSuccessCount
                : Double.NaN;
            return new Stats(newSuccessCount, newPendingCount, newFailedCount, newInferenceTime);
        }

        Stats sub(Stats value) {
            long newSuccessCount = Math.max(0, successCount - value.successCount);
            long newPendingCount = Math.max(0, pendingCount - value.pendingCount);
            long newFailedCount = Math.max(0, failedCount - value.failedCount);
            double newInferenceTime = newSuccessCount > 0
                ? (totalInferenceTime() - value.totalInferenceTime()) / newSuccessCount
                : Double.NaN;
            return new Stats(newSuccessCount, newPendingCount, newFailedCount, newInferenceTime);
        }
    }

    private class Metrics {

        private final List<AutoCloseable> metrics = new ArrayList<>();

        Metrics() {}

        void init() {
            if (metrics.isEmpty() == false) {
                return;
            }
            metrics.add(
                meterRegistry.registerLongsGauge(
                    "es.ml.trained_models.adaptive_allocations.actual_number_of_allocations.current",
                    "the actual number of allocations",
                    "",
                    () -> observeLong(AdaptiveAllocationsScaler::getNumberOfAllocations)
                )
            );
            metrics.add(
                meterRegistry.registerLongsGauge(
                    "es.ml.trained_models.adaptive_allocations.needed_number_of_allocations.current",
                    "the number of allocations needed according to the adaptive allocations scaler",
                    "",
                    () -> observeLong(AdaptiveAllocationsScaler::getNeededNumberOfAllocations)
                )
            );
            metrics.add(
                meterRegistry.registerDoublesGauge(
                    "es.ml.trained_models.adaptive_allocations.measured_request_rate.current",
                    "the request rate reported by the stats API",
                    "1/s",
                    () -> observeDouble(AdaptiveAllocationsScaler::getLastMeasuredRequestRate)
                )
            );
            metrics.add(
                meterRegistry.registerDoublesGauge(
                    "es.ml.trained_models.adaptive_allocations.estimated_request_rate.current",
                    "the request rate estimated by the adaptive allocations scaler",
                    "1/s",
                    () -> observeDouble(AdaptiveAllocationsScaler::getRequestRateEstimate)
                )
            );
            metrics.add(
                meterRegistry.registerDoublesGauge(
                    "es.ml.trained_models.adaptive_allocations.measured_inference_time.current",
                    "the inference time reported by the stats API",
                    "s",
                    () -> observeDouble(AdaptiveAllocationsScaler::getLastMeasuredInferenceTime)
                )
            );
            metrics.add(
                meterRegistry.registerDoublesGauge(
                    "es.ml.trained_models.adaptive_allocations.estimated_inference_time.current",
                    "the inference time estimated by the adaptive allocations scaler",
                    "s",
                    () -> observeDouble(AdaptiveAllocationsScaler::getInferenceTimeEstimate)
                )
            );
            metrics.add(
                meterRegistry.registerLongsGauge(
                    "es.ml.trained_models.adaptive_allocations.queue_size.current",
                    "the queue size reported by the stats API",
                    "s",
                    () -> observeLong(AdaptiveAllocationsScaler::getLastMeasuredQueueSize)
                )
            );
        }

        Collection<LongWithAttributes> observeLong(Function<AdaptiveAllocationsScaler, Long> getValue) {
            List<LongWithAttributes> observations = new ArrayList<>();
            for (AdaptiveAllocationsScaler scaler : scalers.values()) {
                Long value = getValue.apply(scaler);
                if (value != null) {
                    observations.add(new LongWithAttributes(value, Map.of("deployment_id", scaler.getDeploymentId())));
                }
            }
            return observations;
        }

        Collection<DoubleWithAttributes> observeDouble(Function<AdaptiveAllocationsScaler, Double> getValue) {
            List<DoubleWithAttributes> observations = new ArrayList<>();
            for (AdaptiveAllocationsScaler scaler : scalers.values()) {
                Double value = getValue.apply(scaler);
                if (value != null) {
                    observations.add(new DoubleWithAttributes(value, Map.of("deployment_id", scaler.getDeploymentId())));
                }
            }
            return observations;
        }
    }

    /**
     * The time interval between the adaptive allocations triggers.
     */
    private static final int DEFAULT_TIME_INTERVAL_SECONDS = 10;
    /**
     * The time that has to pass after scaling up, before scaling down is allowed.
     * Note that the ML autoscaling has its own cooldown time to release the hardware.
     */
    private static final long SCALE_UP_COOLDOWN_TIME_MILLIS = TimeValue.timeValueMinutes(5).getMillis();

    private static final Logger logger = LogManager.getLogger(AdaptiveAllocationsScalerService.class);

    private final int timeIntervalSeconds;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;
    private final InferenceAuditor inferenceAuditor;
    private final MeterRegistry meterRegistry;
    private final Metrics metrics;
    private final boolean isNlpEnabled;
    private final Map<String, Map<String, Stats>> lastInferenceStatsByDeploymentAndNode;
    private Long lastInferenceStatsTimestampMillis;
    private final Map<String, AdaptiveAllocationsScaler> scalers;
    private final Map<String, Long> lastScaleUpTimesMillis;

    private volatile Scheduler.Cancellable cancellable;
    private final AtomicBoolean busy;

    public AdaptiveAllocationsScalerService(
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        InferenceAuditor inferenceAuditor,
        MeterRegistry meterRegistry,
        boolean isNlpEnabled
    ) {
        this(threadPool, clusterService, client, inferenceAuditor, meterRegistry, isNlpEnabled, DEFAULT_TIME_INTERVAL_SECONDS);
    }

    // visible for testing
    AdaptiveAllocationsScalerService(
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        InferenceAuditor inferenceAuditor,
        MeterRegistry meterRegistry,
        boolean isNlpEnabled,
        int timeIntervalSeconds
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.inferenceAuditor = inferenceAuditor;
        this.meterRegistry = meterRegistry;
        this.isNlpEnabled = isNlpEnabled;
        this.timeIntervalSeconds = timeIntervalSeconds;

        lastInferenceStatsByDeploymentAndNode = new HashMap<>();
        lastInferenceStatsTimestampMillis = null;
        lastScaleUpTimesMillis = new HashMap<>();
        scalers = new HashMap<>();
        metrics = new Metrics();
        busy = new AtomicBoolean(false);
    }

    public synchronized void start() {
        updateAutoscalers(clusterService.state());
        metrics.init();
        clusterService.addListener(this);
        if (scalers.isEmpty() == false) {
            startScheduling();
        }
    }

    public synchronized void stop() {
        clusterService.removeListener(this);
        stopScheduling();
        scalers.clear();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return;
        }

        updateAutoscalers(event.state());
        if (scalers.isEmpty() == false) {
            startScheduling();
        } else {
            stopScheduling();
        }
    }

    private synchronized void updateAutoscalers(ClusterState state) {
        if (isNlpEnabled == false) {
            return;
        }
        Set<String> deploymentIds = new HashSet<>();
        TrainedModelAssignmentMetadata assignments = TrainedModelAssignmentMetadata.fromState(state);
        for (TrainedModelAssignment assignment : assignments.allAssignments().values()) {
            deploymentIds.add(assignment.getDeploymentId());
            if (assignment.getAdaptiveAllocationsSettings() != null
                && assignment.getAdaptiveAllocationsSettings().getEnabled() == Boolean.TRUE) {
                AdaptiveAllocationsScaler adaptiveAllocationsScaler = scalers.computeIfAbsent(
                    assignment.getDeploymentId(),
                    key -> new AdaptiveAllocationsScaler(assignment.getDeploymentId(), assignment.totalTargetAllocations())
                );
                adaptiveAllocationsScaler.setMinMaxNumberOfAllocations(
                    assignment.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(),
                    assignment.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations()
                );
            } else {
                scalers.remove(assignment.getDeploymentId());
                lastInferenceStatsByDeploymentAndNode.remove(assignment.getDeploymentId());
            }
        }
        scalers.keySet().removeIf(key -> deploymentIds.contains(key) == false);
    }

    private synchronized void startScheduling() {
        if (cancellable == null) {
            logger.debug("Starting ML adaptive allocations scaler");
            try {
                cancellable = threadPool.scheduleWithFixedDelay(
                    this::trigger,
                    TimeValue.timeValueSeconds(timeIntervalSeconds),
                    threadPool.generic()
                );
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown() == false) {
                    throw e;
                }
            }
        }
    }

    private synchronized void stopScheduling() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            logger.debug("Stopping ML adaptive allocations scaler");
            cancellable.cancel();
            cancellable = null;
        }
    }

    private void trigger() {
        if (busy.getAndSet(true)) {
            logger.debug("Skipping inference adaptive allocations scaling, because it's still busy.");
            return;
        }
        ActionListener<GetDeploymentStatsAction.Response> listener = ActionListener.runAfter(
            ActionListener.wrap(this::processDeploymentStats, e -> logger.warn("Error in inference adaptive allocations scaling", e)),
            () -> busy.set(false)
        );
        getDeploymentStats(listener);
    }

    private void getDeploymentStats(ActionListener<GetDeploymentStatsAction.Response> processDeploymentStats) {
        String deploymentIds = String.join(",", scalers.keySet());
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            GetDeploymentStatsAction.INSTANCE,
            // TODO(dave/jan): create a lightweight version of this request, because the current one
            // collects too much data for the adaptive allocations scaler.
            new GetDeploymentStatsAction.Request(deploymentIds),
            processDeploymentStats
        );
    }

    private void processDeploymentStats(GetDeploymentStatsAction.Response statsResponse) {
        Double statsTimeInterval;
        long now = System.currentTimeMillis();
        if (lastInferenceStatsTimestampMillis != null) {
            statsTimeInterval = (now - lastInferenceStatsTimestampMillis) / 1000.0;
        } else {
            statsTimeInterval = null;
        }
        lastInferenceStatsTimestampMillis = now;

        Map<String, Stats> recentStatsByDeployment = new HashMap<>();
        Map<String, Integer> numberOfAllocations = new HashMap<>();

        for (AssignmentStats assignmentStats : statsResponse.getStats().results()) {
            String deploymentId = assignmentStats.getDeploymentId();
            numberOfAllocations.put(deploymentId, assignmentStats.getNumberOfAllocations());
            Map<String, Stats> deploymentStats = lastInferenceStatsByDeploymentAndNode.computeIfAbsent(
                deploymentId,
                key -> new HashMap<>()
            );
            for (AssignmentStats.NodeStats nodeStats : assignmentStats.getNodeStats()) {
                String nodeId = nodeStats.getNode().getId();
                Stats lastStats = deploymentStats.get(nodeId);
                Stats nextStats = new Stats(
                    nodeStats.getInferenceCount().orElse(0L),
                    nodeStats.getPendingCount() == null ? 0 : nodeStats.getPendingCount(),
                    nodeStats.getErrorCount() + nodeStats.getTimeoutCount() + nodeStats.getRejectedExecutionCount(),
                    nodeStats.getAvgInferenceTime().orElse(0.0) / 1000.0
                );
                deploymentStats.put(nodeId, nextStats);
                if (lastStats != null) {
                    Stats recentStats = nextStats.sub(lastStats);
                    recentStatsByDeployment.compute(
                        assignmentStats.getDeploymentId(),
                        (key, value) -> value == null ? recentStats : value.add(recentStats)
                    );
                }
            }
        }

        if (statsTimeInterval == null) {
            return;
        }

        for (Map.Entry<String, Stats> deploymentAndStats : recentStatsByDeployment.entrySet()) {
            String deploymentId = deploymentAndStats.getKey();
            Stats stats = deploymentAndStats.getValue();
            AdaptiveAllocationsScaler adaptiveAllocationsScaler = scalers.get(deploymentId);
            adaptiveAllocationsScaler.process(stats, statsTimeInterval, numberOfAllocations.get(deploymentId));
            Integer newNumberOfAllocations = adaptiveAllocationsScaler.scale();
            if (newNumberOfAllocations != null) {
                Long lastScaleUpTimeMillis = lastScaleUpTimesMillis.get(deploymentId);
                if (newNumberOfAllocations < numberOfAllocations.get(deploymentId)
                    && lastScaleUpTimeMillis != null
                    && now < lastScaleUpTimeMillis + SCALE_UP_COOLDOWN_TIME_MILLIS) {
                    logger.debug("adaptive allocations scaler: skipping scaling down [{}] because of recent scaleup.", deploymentId);
                    continue;
                }
                if (newNumberOfAllocations > numberOfAllocations.get(deploymentId)) {
                    lastScaleUpTimesMillis.put(deploymentId, now);
                }
                updateNumberOfAllocations(deploymentId, newNumberOfAllocations);
            }
        }
    }

    public boolean maybeStartAllocation(TrainedModelAssignment assignment) {
        if (assignment.getAdaptiveAllocationsSettings() != null
            && assignment.getAdaptiveAllocationsSettings().getEnabled() == Boolean.TRUE) {
            lastScaleUpTimesMillis.put(assignment.getDeploymentId(), System.currentTimeMillis());
            updateNumberOfAllocations(assignment.getDeploymentId(), 1);
            return true;
        }
        return false;
    }

    private void updateNumberOfAllocations(String deploymentId, int numberOfAllocations) {
        UpdateTrainedModelDeploymentAction.Request updateRequest = new UpdateTrainedModelDeploymentAction.Request(deploymentId);
        updateRequest.setNumberOfAllocations(numberOfAllocations);
        updateRequest.setIsInternal(true);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            UpdateTrainedModelDeploymentAction.INSTANCE,
            updateRequest,
            ActionListener.wrap(updateResponse -> {
                logger.info("adaptive allocations scaler: scaled [{}] to [{}] allocations.", deploymentId, numberOfAllocations);
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                    .execute(
                        () -> inferenceAuditor.info(
                            deploymentId,
                            Strings.format(
                                "adaptive allocations scaler: scaled [%s] to [%s] allocations.",
                                deploymentId,
                                numberOfAllocations
                            )
                        )
                    );
            }, e -> {
                logger.atLevel(Level.WARN)
                    .withThrowable(e)
                    .log("adaptive allocations scaler: scaling [{}] to [{}] allocations failed.", deploymentId, numberOfAllocations);
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                    .execute(
                        () -> inferenceAuditor.warning(
                            deploymentId,
                            Strings.format(
                                "adaptive allocations scaler: scaling [%s] to [%s] allocations failed.",
                                deploymentId,
                                numberOfAllocations
                            )
                        )
                    );
            })
        );
    }
}
