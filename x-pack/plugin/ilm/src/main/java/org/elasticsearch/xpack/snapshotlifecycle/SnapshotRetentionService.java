/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;

import java.io.Closeable;
import java.time.Clock;
import java.util.function.Supplier;

/**
 * The {@code SnapshotRetentionService} is responsible for scheduling the period kickoff of SLM's
 * snapshot retention. This means that when the retention schedule setting is configured, the
 * scheduler schedules a job that, when triggered, will delete snapshots according to the retention
 * policy configured in the {@link SnapshotLifecyclePolicy}.
 */
public class SnapshotRetentionService implements LocalNodeMasterListener, Closeable {

    static final String SLM_RETENTION_JOB_ID = "slm-retention-job";

    private static final Logger logger = LogManager.getLogger(SnapshotRetentionService.class);

    private final SchedulerEngine scheduler;

    private volatile String slmRetentionSchedule;

    public SnapshotRetentionService(Settings settings,
                                    Supplier<SnapshotRetentionTask> taskSupplier,
                                    ClusterService clusterService,
                                    Clock clock) {
        this.scheduler = new SchedulerEngine(settings, clock);
        this.scheduler.register(taskSupplier.get());
        this.slmRetentionSchedule = LifecycleSettings.SLM_RETENTION_SCHEDULE_SETTING.get(settings);
        clusterService.addLocalNodeMasterListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LifecycleSettings.SLM_RETENTION_SCHEDULE_SETTING,
            this::setUpdateSchedule);
    }

    void setUpdateSchedule(String retentionSchedule) {
        this.slmRetentionSchedule = retentionSchedule;
        // The schedule has changed, so reschedule the retention job
        rescheduleRetentionJob();
    }

    // Only used for testing
    SchedulerEngine getScheduler() {
        return this.scheduler;
    }

    @Override
    public void onMaster() {
        rescheduleRetentionJob();
    }

    @Override
    public void offMaster() {
        cancelRetentionJob();
    }

    private void rescheduleRetentionJob() {
        final String schedule = this.slmRetentionSchedule;
        if (Strings.hasText(schedule)) {
            final SchedulerEngine.Job retentionJob = new SchedulerEngine.Job(SLM_RETENTION_JOB_ID,
                new CronSchedule(schedule));
            logger.debug("scheduling SLM retention job for [{}]", schedule);
            this.scheduler.add(retentionJob);
        } else {
            // The schedule has been unset, so cancel the scheduled retention job
            cancelRetentionJob();
        }
    }

    private void cancelRetentionJob() {
        this.scheduler.scheduledJobIds().forEach(this.scheduler::remove);
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    public void close() {
        this.scheduler.stop();
    }
}
