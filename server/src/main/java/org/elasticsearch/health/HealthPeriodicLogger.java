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
import java.util.Map;


public class HealthPeriodicLogger implements ClusterStateListener, Closeable, SchedulerEngine.Listener {
    public static final String HEALTH_SERVICE_POLL_INTERVAL = "health_service.poll_interval";
    public static final Setting<TimeValue> HEALTH_SERVICE_POLL_INTERVAL_SETTING = Setting.timeSetting(
        HEALTH_SERVICE_POLL_INTERVAL,
        TimeValue.timeValueSeconds(15),
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Name constant for the job HealthService schedules
     */
    private static final String HEALTH_SERVICE_JOB_NAME = "health_service";

    private final Settings settings;

    private final ClusterService clusterService;
    private final NodeClient client;

    private final HealthService healthService;
    private final Clock clock;

    private volatile boolean isHealthNode = false;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private volatile TimeValue pollInterval;

    private static final Logger logger = LogManager.getLogger(HealthPeriodicLogger.class);

    public HealthPeriodicLogger(
        Settings settings,
        ClusterService clusterService,
        NodeClient client,
        HealthService healthService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.client = client;
        this.healthService = healthService;
        this.clock = getClock();
        this.scheduledJob = null;
        this.pollInterval = HEALTH_SERVICE_POLL_INTERVAL_SETTING.get(settings);
    }


    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(HEALTH_SERVICE_POLL_INTERVAL_SETTING, this::updatePollInterval);
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
            return;
        }
        final boolean isCurrentlyHealthNode = healthNode.getId().equals(this.clusterService.localNode().getId());

        final boolean prevIsHealthNode = this.isHealthNode;
        if (prevIsHealthNode != isCurrentlyHealthNode) {
            this.isHealthNode = isCurrentlyHealthNode;
            if (this.isHealthNode) {
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
            scheduler.get().remove(HEALTH_SERVICE_JOB_NAME);
            scheduledJob = null;
        }
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private void maybeScheduleJob() {
        if (this.isHealthNode == false) {
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
        scheduledJob = new SchedulerEngine.Job(HEALTH_SERVICE_JOB_NAME, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);

    }


    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(HEALTH_SERVICE_JOB_NAME)) {
            if (this.isHealthNode) {

                this.healthService.getHealth(this.client, null, true, 0, new ActionListener<List<HealthIndicatorResult>>() {
                    @Override
                    public void onResponse(List<HealthIndicatorResult> healthIndicatorResults) {
                        GetHealthAction.Response healthResponse = new GetHealthAction.Response(null, healthIndicatorResults, true);
                        Map<String, Object> jsonFields = new HashMap<>();
                        jsonFields.put("elasticsearch.health.status", healthResponse.getStatus().xContentValue());
                        healthResponse.getIndicatorResults().forEach(
                            (result) -> {
                                jsonFields.put(
                                    String.format("elasticsearch.health.%s.status", result.name()),
                                    result.status().xContentValue()
                                );
                            }
                        );
                        ESLogMessage msg = new ESLogMessage().withFields(jsonFields);
                        logger.info(msg);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Health Service logging error:{}", e.toString());
                    }
                });
            }
        }
    }

}
