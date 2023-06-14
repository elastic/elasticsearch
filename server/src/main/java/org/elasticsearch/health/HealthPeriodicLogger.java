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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class HealthPeriodicLogger implements ClusterStateListener, Closeable, SchedulerEngine.Listener {

    public static class HealthPeriodicLoggerResult {
        private final List<HealthIndicatorResult> indicatorResults;

        public HealthPeriodicLoggerResult(List<HealthIndicatorResult> indicatorResults) {
            this.indicatorResults = indicatorResults;
        }

        private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
            NamedXContentRegistry registry = new NamedXContentRegistry(
                CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())
            );
            XContentBuilder builder = XContentFactory.jsonBuilder();
            xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(registry, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            return parser.map();
        }

        public Map<String, Object> toMap() {
            final Map<String, Object> result = new HashMap<>();

            // overall status
            final HealthStatus status = HealthStatus.merge(this.indicatorResults.stream().map(HealthIndicatorResult::status));
            result.put("elasticsearch.health.status", status.xContentValue());

            // top-level status for each indicator
            this.indicatorResults.forEach((indicatorResult) -> {
                result.put(
                    String.format(Locale.ROOT, "elasticsearch.health.%s.status", indicatorResult.name()),
                    indicatorResult.status().xContentValue()
                );
            });

            return result;
        }
    }

    public static final String HEALTH_PERIODIC_LOGGER_POLL_INTERVAL = "health_periodic_logger.poll_interval";
    public static final Setting<TimeValue> HEALTH_PERIODIC_LOGGER_POLL_INTERVAL_SETTING = Setting.timeSetting(
        HEALTH_PERIODIC_LOGGER_POLL_INTERVAL,
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Name constant for the job HealthService schedules
     */
    private static final String HEALTH_PERIODIC_LOGGER_JOB_NAME = "health_periodic_logger";

    private final Settings settings;

    private final ClusterService clusterService;
    private final NodeClient client;

    private final HealthService healthService;
    private final Clock clock;

    private final AtomicBoolean isHealthNode = new AtomicBoolean(false);
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private volatile TimeValue pollInterval;

    private static final Logger logger = LogManager.getLogger(HealthPeriodicLogger.class);

    private final ActionListener<List<HealthIndicatorResult>> resultsListener = new ActionListener<List<HealthIndicatorResult>>() {
        @Override
        public void onResponse(List<HealthIndicatorResult> healthIndicatorResults) {
            HealthPeriodicLoggerResult result = new HealthPeriodicLoggerResult(healthIndicatorResults);
            ESLogMessage msg = new ESLogMessage().withFields(result.toMap());
            logger.info(msg);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Health Service logging error:{}", e.toString());
        }

    };

    public HealthPeriodicLogger(Settings settings, ClusterService clusterService, NodeClient client, HealthService healthService) {
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

        final boolean prevIsHealthNode = this.isHealthNode.get();
        if (prevIsHealthNode != isCurrentlyHealthNode) {
            this.isHealthNode.set(isCurrentlyHealthNode);
            if (this.isHealthNode.get()) {
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
        if (this.isHealthNode.get() == false) {
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

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(HEALTH_PERIODIC_LOGGER_JOB_NAME)) {
            if (this.isHealthNode.get()) {
                this.healthService.getHealth(this.client, null, true, 0, resultsListener);
            }
        }
    }

}
