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
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.health.node.selection.HealthNode;

import java.io.Closeable;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class periodically logs the results of the Health API to the standard Elasticsearch server log file.
 */
public class HealthPeriodicLogger implements ClusterStateListener, Closeable, SchedulerEngine.Listener {
    public static final String HEALTH_FIELD_PREFIX = "elasticsearch.health";

    /**
     * Creates a new HealthPeriodicLoggerResult.
     * This creates a record that's used as the output of this feature.
     *
     * @param indicatorResults the results of the Health API call that will be used as the output.
     */
    public record HealthPeriodicLoggerResult(List<HealthIndicatorResult> indicatorResults) {

        /**
         * Create a Map of the results, which is then turned into JSON for logging
         */
        public Map<String, Object> toMap() {
            final Map<String, Object> result = new HashMap<>();

            if (this.indicatorResults != null && this.indicatorResults.size() > 0) {
                // overall status
                final HealthStatus status = HealthStatus.merge(this.indicatorResults.stream().map(HealthIndicatorResult::status));
                result.put("elasticsearch.health.status", status.xContentValue());

                // top-level status for each indicator
                this.indicatorResults.forEach((indicatorResult) -> {
                    result.put(
                        String.format(Locale.ROOT, "%s.%s.status", HEALTH_FIELD_PREFIX, indicatorResult.name()),
                        indicatorResult.status().xContentValue()
                    );
                });
            }

            return result;
        }
    }

    public static final String HEALTH_PERIODIC_LOGGER_POLL_INTERVAL = "health_periodic_logger.poll_interval";
    public static final Setting<TimeValue> HEALTH_PERIODIC_LOGGER_POLL_INTERVAL_SETTING = Setting.timeSetting(
        HEALTH_PERIODIC_LOGGER_POLL_INTERVAL,
        TimeValue.timeValueSeconds(15),
        TimeValue.timeValueSeconds(15),
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

    private final AtomicBoolean isHealthNode = new AtomicBoolean(false);
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private volatile TimeValue pollInterval;

    private static final Logger logger = LogManager.getLogger(HealthPeriodicLogger.class);

    /**
     * Creates a new HealthPeriodicLogger.
     * This creates a scheduled job using the SchedulerEngine framework and runs it on the current health node.
     *
     * @param settings the cluster settings, used to get the interval setting.
     * @param clusterService the cluster service, used to know when the health node changes.
     * @param client the client used to call the Health Service.
     * @param healthService the Health Service, where the actual Health API logic lives.
     */
    public HealthPeriodicLogger(Settings settings, ClusterService clusterService, Client client, HealthService healthService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.client = client;
        this.healthService = healthService;
        this.clock = getClock();
        this.pollInterval = HEALTH_PERIODIC_LOGGER_POLL_INTERVAL_SETTING.get(settings);
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(HEALTH_PERIODIC_LOGGER_POLL_INTERVAL_SETTING, this::updatePollInterval);
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        DiscoveryNode healthNode = HealthNode.findHealthNode(event.state());
        if (healthNode == null) {
            this.cancelJob();
            return;
        }
        final boolean isCurrentlyHealthNode = healthNode.getId().equals(this.clusterService.localNode().getId());
        final boolean prevIsHealthNode = this.getIsHealthNode();
        if (prevIsHealthNode != isCurrentlyHealthNode) {
            this.isHealthNode.set(isCurrentlyHealthNode);
            if (this.getIsHealthNode()) {
                // we weren't the health node, and now we are
                maybeScheduleJob();
            } else {
                // we were the health node, and now we aren't
                cancelJob();
            }
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(HEALTH_PERIODIC_LOGGER_JOB_NAME)) {
            if (this.getIsHealthNode()) {
                this.callGetHealth();
            }
        }
    }

    protected void callGetHealth() {
        this.healthService.getHealth(this.client, null, false, 0, this.resultsListener);
    }

    protected boolean getIsHealthNode() {
        return isHealthNode.get();
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(HEALTH_PERIODIC_LOGGER_JOB_NAME);
        }
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private void maybeScheduleJob() {
        if (this.getIsHealthNode() == false) {
            return;
        }

        // don't schedule the job if the node is shutting down
        if (isClusterServiceStoppedOrClosed()) {
            logger.trace(
                "Skipping scheduling a HealthService job due to the cluster lifecycle state being: [{}] ",
                clusterService.lifecycleState()
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

    private final ActionListener<List<HealthIndicatorResult>> resultsListener = new ActionListener<List<HealthIndicatorResult>>() {
        @Override
        public void onResponse(List<HealthIndicatorResult> healthIndicatorResults) {
            HealthPeriodicLoggerResult result = new HealthPeriodicLoggerResult(healthIndicatorResults);
            Map<String, Object> resultsMap = result.toMap();
            if (resultsMap.size() > 0) {
                ESLogMessage msg = new ESLogMessage().withFields(result.toMap());
                logger.info(msg);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Health Service logging error:{}", e.toString());
        }
    };
}
