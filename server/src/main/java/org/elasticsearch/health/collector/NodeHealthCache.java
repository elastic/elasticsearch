/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.collector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/**
 * Main component responsible for caching the health state of the nodes
 * of the cluster.
 */
public class NodeHealthCache extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(NodeHealthCache.class);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "node.health.state.poll.interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final String NODE_HEALTH_STATE_COLLECTOR = "node-health-cache";

    private final ThreadPool threadPool;

    private volatile TimeValue pollInterval;
    private volatile Scheduler.ScheduledCancellable scheduled;

    NodeHealthCache(
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
        this.threadPool = threadPool;
        pollInterval = POLL_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
    }

    public void setPollInterval(TimeValue pollInterval) {
        this.pollInterval = pollInterval;
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRun(new TimeValue(1));
        }
    }

    void run() {
        if (isCancelled() || isCompleted()) {
            return;
        }
        logger.info("Collecting stuff, lalalala");
        scheduleNextRun(pollInterval);
    }

    @Override
    protected void onCancelled() {
        if (scheduled != null) {
            scheduled.cancel();
        }
        markAsCompleted();
    }

    private void scheduleNextRun(TimeValue time) {
        if (threadPool.scheduler().isShutdown() == false) {
            scheduled = threadPool.schedule(this::run, time, ThreadPool.Names.GENERIC);
        }
    }
}
