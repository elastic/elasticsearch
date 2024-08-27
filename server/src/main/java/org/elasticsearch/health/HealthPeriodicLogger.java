/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.time.Clock;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;

/**
 * This class periodically logs the results of the Health API to the standard Elasticsearch server log file. It a lifecycle
 * aware component because it health depends on other lifecycle aware components. This means:
 * - We do not schedule any jobs until the lifecycle state is STARTED
 * - When the lifecycle state becomes STOPPED, do not schedule any more runs, but we do let the current one finish
 * - When the lifecycle state becomes CLOSED, we will interrupt the current run as well.
 */
public class HealthPeriodicLogger extends AbstractLifecycleComponent implements ClusterStateListener, SchedulerEngine.Listener {
    public static final String HEALTH_FIELD_PREFIX = "elasticsearch.health";
    public static final String MESSAGE_FIELD = "message";

    /**
     * Valid modes of output for this logger
     */
    public enum OutputMode {
        LOGS("logs"),
        METRICS("metrics");

        private final String mode;

        OutputMode(String mode) {
            this.mode = mode;
        }

        public static OutputMode fromString(String mode) {
            return valueOf(mode.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return this.mode.toLowerCase(Locale.ROOT);
        }

        static OutputMode parseOutputMode(String value) {
            try {
                return OutputMode.fromString(value);
            } catch (Exception e) {
                throw new IllegalArgumentException("Illegal OutputMode:" + value);
            }
        }
    }

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "health.periodic_logger.poll_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.periodic_logger.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<List<OutputMode>> OUTPUT_MODE_SETTING = Setting.listSetting(
        "health.periodic_logger.output_mode",
        List.of(OutputMode.LOGS.toString(), OutputMode.METRICS.toString()),
        OutputMode::parseOutputMode,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Name constant for the job HealthService schedules
     */
    protected static final String HEALTH_PERIODIC_LOGGER_JOB_NAME = "health_periodic_logger";

    private final Settings settings;

    private final ClusterService clusterService;
    private final Client client;

    private final HealthService healthService;
    private final Clock clock;

    private volatile boolean isHealthNode = false;

    // This semaphore is used to ensure only one schedule is currently running and to wait for this run to finish before closing
    private final Semaphore currentlyRunning = new Semaphore(1, true);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private volatile TimeValue pollInterval;
    private volatile boolean enabled;
    private volatile Set<OutputMode> outputModes;

    private static final Logger logger = LogManager.getLogger(HealthPeriodicLogger.class);

    private final MeterRegistry meterRegistry;
    private final Map<String, LongGaugeMetric> redMetrics = new HashMap<>();

    // Writers for logs or messages
    // default visibility for testing purposes
    private final BiConsumer<LongGaugeMetric, Long> metricWriter;
    private final Consumer<ESLogMessage> logWriter;

    /**
     * Creates a new HealthPeriodicLogger.
     * This creates a scheduled job using the SchedulerEngine framework and runs it on the current health node.
     *
     * @param settings the cluster settings, used to get the interval setting.
     * @param clusterService the cluster service, used to know when the health node changes.
     * @param client the client used to call the Health Service.
     * @param healthService the Health Service, where the actual Health API logic lives.
     * @param telemetryProvider used to get the meter registry for metrics
     */
    public static HealthPeriodicLogger create(
        Settings settings,
        ClusterService clusterService,
        Client client,
        HealthService healthService,
        TelemetryProvider telemetryProvider
    ) {
        return HealthPeriodicLogger.create(settings, clusterService, client, healthService, telemetryProvider, null, null);
    }

    static HealthPeriodicLogger create(
        Settings settings,
        ClusterService clusterService,
        Client client,
        HealthService healthService,
        TelemetryProvider telemetryProvider,
        BiConsumer<LongGaugeMetric, Long> metricWriter,
        Consumer<ESLogMessage> logWriter
    ) {
        HealthPeriodicLogger healthLogger = new HealthPeriodicLogger(
            settings,
            clusterService,
            client,
            healthService,
            telemetryProvider.getMeterRegistry(),
            metricWriter,
            logWriter
        );
        healthLogger.registerListeners();
        return healthLogger;
    }

    private HealthPeriodicLogger(
        Settings settings,
        ClusterService clusterService,
        Client client,
        HealthService healthService,
        MeterRegistry meterRegistry,
        BiConsumer<LongGaugeMetric, Long> metricWriter,
        Consumer<ESLogMessage> logWriter
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.client = client;
        this.healthService = healthService;
        this.clock = Clock.systemUTC();
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.enabled = ENABLED_SETTING.get(settings);
        this.outputModes = EnumSet.copyOf(OUTPUT_MODE_SETTING.get(settings));
        this.meterRegistry = meterRegistry;
        this.metricWriter = metricWriter == null ? LongGaugeMetric::set : metricWriter;
        this.logWriter = logWriter == null ? logger::info : logWriter;

        // create metric for overall level metrics
        this.redMetrics.put(
            "overall",
            LongGaugeMetric.create(this.meterRegistry, "es.health.overall.red.status", "Overall: Red", "{cluster}")
        );
    }

    private void registerListeners() {
        if (enabled) {
            clusterService.addListener(this);
        }
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::enable);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(OUTPUT_MODE_SETTING, this::updateOutputModes);
        this.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                maybeScheduleJob();
            }

            @Override
            public void afterStop() {
                maybeCancelJob();
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        DiscoveryNode healthNode = HealthNode.findHealthNode(event.state());
        if (healthNode == null) {
            this.isHealthNode = false;
            this.maybeCancelJob();
            return;
        }
        final boolean isCurrentlyHealthNode = healthNode.getId().equals(this.clusterService.localNode().getId());
        if (this.isHealthNode != isCurrentlyHealthNode) {
            this.isHealthNode = isCurrentlyHealthNode;
            if (this.isHealthNode) {
                // we weren't the health node, and now we are
                maybeScheduleJob();
            } else {
                // we were the health node, and now we aren't
                maybeCancelJob();
            }
        }
    }

    @Override
    protected void doStart() {
        logger.debug("Periodic health logger is starting.");
    }

    /**
     * Stopping means that the periodic health logger will not schedule any more runs. If the logger is currently running it will
     * let this run finish, but it will cancel any future scheduling, and it will deregister the cluster state listener.
     */
    @Override
    protected void doStop() {
        clusterService.removeListener(this);
        logger.debug("Periodic health logger is stopping.");
    }

    @Override
    protected void doClose() throws IOException {
        logger.debug("Periodic health logger is closing.");
        try {
            // The health API is expected to be a quick call, so we do not need a very long timeout
            if (currentlyRunning.tryAcquire(2, TimeUnit.SECONDS)) {
                logger.debug("Periodic health logger's last run has successfully finished.");
            }
        } catch (InterruptedException e) {
            logger.warn("Error while waiting for the last run of the periodic health logger to finish.", e);
        } finally {
            SchedulerEngine engine = scheduler.get();
            if (engine != null) {
                engine.stop();
            }
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.jobName().equals(HEALTH_PERIODIC_LOGGER_JOB_NAME) && this.enabled) {
            this.tryToLogHealth();
        }
    }

    // default visibility and returns true if the semaphore was acquired, used only for testing
    boolean tryToLogHealth() {
        try {
            // We only try to run this because we do not want to pile up the executions.
            if (currentlyRunning.tryAcquire(0, TimeUnit.SECONDS)) {
                RunOnce release = new RunOnce(currentlyRunning::release);
                try {
                    ActionListener<List<HealthIndicatorResult>> listenerWithRelease = ActionListener.runAfter(resultsListener, release);
                    this.healthService.getHealth(this.client, null, true, 0, listenerWithRelease);
                } catch (Exception e) {
                    // In case of an exception before the listener was wired, we can release the flag here, and we feel safe
                    // that it will not release it again because this can only be run once.
                    release.run();
                    logger.warn(() -> "The health periodic logger encountered an error.", e);
                }
                return true;
            } else {
                logger.debug("Skipping this run because it's already in progress.");
            }
        } catch (InterruptedException e) {
            logger.debug("Periodic health logger run was interrupted.", e);
        }
        return false;
    }

    // default visibility for testing purposes
    SchedulerEngine getScheduler() {
        return this.scheduler.get();
    }

    /**
     * Create a Map of the results, which is then turned into JSON for logging.
     * The structure looks like:
     * {"elasticsearch.health.overall.status": "green", "elasticsearch.health.[other indicators].status": "green"}
     * Only the indicator status values are included, along with the computed top-level status.
     *
     * @param indicatorResults the results of the Health API call that will be used as the output.
     */
    // default visibility for testing purposes
    static Map<String, Object> convertToLoggedFields(List<HealthIndicatorResult> indicatorResults) {
        if (indicatorResults == null || indicatorResults.isEmpty()) {
            return Map.of();
        }

        final Map<String, Object> result = new HashMap<>();

        // overall status
        final HealthStatus status = calculateOverallStatus(indicatorResults);
        result.put(String.format(Locale.ROOT, "%s.overall.status", HEALTH_FIELD_PREFIX), status.xContentValue());

        // top-level status for each indicator
        indicatorResults.forEach((indicatorResult) -> {
            result.put(
                String.format(Locale.ROOT, "%s.%s.status", HEALTH_FIELD_PREFIX, indicatorResult.name()),
                indicatorResult.status().xContentValue()
            );
            if (GREEN.equals(indicatorResult.status()) == false && indicatorResult.details() != null) {
                result.put(
                    String.format(Locale.ROOT, "%s.%s.details", HEALTH_FIELD_PREFIX, indicatorResult.name()),
                    Strings.toString(indicatorResult.details())
                );
            }
        });

        // message field. Show the non-green indicators if they exist.
        List<String> nonGreen = indicatorResults.stream()
            .filter(p -> p.status() != GREEN)
            .map(HealthIndicatorResult::name)
            .sorted()
            .toList();
        if (nonGreen.isEmpty()) {
            result.put(MESSAGE_FIELD, String.format(Locale.ROOT, "health=%s", status.xContentValue()));
        } else {
            result.put(MESSAGE_FIELD, String.format(Locale.ROOT, "health=%s [%s]", status.xContentValue(), String.join(",", nonGreen)));
        }

        return result;
    }

    static HealthStatus calculateOverallStatus(List<HealthIndicatorResult> indicatorResults) {
        return HealthStatus.merge(indicatorResults.stream().map(HealthIndicatorResult::status));
    }

    /**
     * Handle the result of the Health Service getHealth call
     */
    // default visibility for testing purposes
    final ActionListener<List<HealthIndicatorResult>> resultsListener = new ActionListener<List<HealthIndicatorResult>>() {
        @Override
        public void onResponse(List<HealthIndicatorResult> healthIndicatorResults) {
            try {
                if (logsEnabled()) {
                    Map<String, Object> resultsMap = convertToLoggedFields(healthIndicatorResults);

                    // if we have a valid response, log in JSON format
                    if (resultsMap.isEmpty() == false) {
                        ESLogMessage msg = new ESLogMessage().withFields(resultsMap);
                        logWriter.accept(msg);
                    }
                }

                // handle metrics
                if (metricsEnabled()) {
                    writeMetrics(healthIndicatorResults);
                }

            } catch (Exception e) {
                logger.warn("Health Periodic Logger error:{}", e.toString());
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Health Periodic Logger error:{}", e.toString());
        }
    };

    /**
     * Write (and possibly create) the APM metrics
     */
    // default visibility for testing purposes
    void writeMetrics(List<HealthIndicatorResult> healthIndicatorResults) {
        if (healthIndicatorResults != null) {
            for (HealthIndicatorResult result : healthIndicatorResults) {
                String metricName = result.name();
                LongGaugeMetric metric = this.redMetrics.get(metricName);
                if (metric == null) {
                    metric = LongGaugeMetric.create(
                        this.meterRegistry,
                        String.format(Locale.ROOT, "es.health.%s.red.status", metricName),
                        String.format(Locale.ROOT, "%s: Red", metricName),
                        "{cluster}"
                    );
                    this.redMetrics.put(metricName, metric);
                }
                metricWriter.accept(metric, result.status() == RED ? 1L : 0L);
            }

            metricWriter.accept(this.redMetrics.get("overall"), calculateOverallStatus(healthIndicatorResults) == RED ? 1L : 0L);
        }
    }

    private void updateOutputModes(List<OutputMode> newMode) {
        this.outputModes = EnumSet.copyOf(newMode);
    }

    /**
     * Returns true if any of the outputModes are set to logs
     */
    private boolean logsEnabled() {
        return this.outputModes.contains(OutputMode.LOGS);
    }

    /**
     * Returns true if any of the outputModes are set to metrics
     */
    private boolean metricsEnabled() {
        return this.outputModes.contains(OutputMode.METRICS);
    }

    /**
     * Create the SchedulerEngine.Job if this node is the health node
     */
    private void maybeScheduleJob() {
        if (this.isHealthNode == false) {
            return;
        }

        if (this.enabled == false) {
            return;
        }

        // don't schedule the job if the node is not started yet, or it's shutting down
        if (isStarted() == false) {
            logger.trace(
                "Skipping scheduling a health periodic logger job due to the health logger lifecycle state being: [{}] ",
                this.lifecycleState()
            );
            return;
        }

        if (scheduler.get() == null) {
            scheduler.set(new SchedulerEngine(settings, clock));
            scheduler.get().register(this);
        }

        assert scheduler.get() != null : "scheduler should be available";
        final SchedulerEngine.Job scheduledJob = new SchedulerEngine.Job(
            HEALTH_PERIODIC_LOGGER_JOB_NAME,
            new TimeValueSchedule(pollInterval)
        );
        scheduler.get().add(scheduledJob);
    }

    private void maybeCancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(HEALTH_PERIODIC_LOGGER_JOB_NAME);
        }
    }

    private void enable(boolean enabled) {
        this.enabled = enabled;
        // After the health logger is stopped we do not want to reschedule it
        if (enabled & isStoppedOrClosed() == false) {
            clusterService.addListener(this);
            maybeScheduleJob();
        } else {
            clusterService.removeListener(this);
            maybeCancelJob();
        }
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        // After the health logger is stopped we do not want to reschedule it
        if (isStoppedOrClosed() == false) {
            maybeScheduleJob();
        }
    }

    private boolean isStarted() {
        return lifecycleState() == Lifecycle.State.STARTED;
    }

    private boolean isStoppedOrClosed() {
        return lifecycleState() == Lifecycle.State.STOPPED || lifecycleState() == Lifecycle.State.CLOSED;
    }

    // Visible for testing
    TimeValue getPollInterval() {
        return pollInterval;
    }

    // Visible for testing
    boolean isHealthNode() {
        return isHealthNode;
    }

    // Visible for testing
    boolean enabled() {
        return enabled;
    }

    // Visible for testing
    boolean currentlyRunning() {
        return currentlyRunning.availablePermits() == 0;
    }

    // Visible for testing
    boolean waitingToFinishCurrentRun() {
        return currentlyRunning.hasQueuedThreads();
    }
}
