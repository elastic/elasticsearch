/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Map;

/**
 * {@code SnapshotLifecycleService} manages snapshot policy scheduling and triggering of the
 * {@link SnapshotLifecycleTask}. It reacts to new policies in the cluster state by scheduling a
 * task according to the policy's schedule.
 */
public class SnapshotLifecycleService implements LocalNodeMasterListener, Closeable, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotLifecycleMetadata.class);

    private final SchedulerEngine scheduler;
    private final ClusterService clusterService;
    private final SnapshotLifecycleTask snapshotTask;
    private final Map<String, SchedulerEngine.Job> scheduledTasks = ConcurrentCollections.newConcurrentMap();
    private volatile boolean isMaster = false;

    public SnapshotLifecycleService(Settings settings, Client client, ClusterService clusterService,
                                    Clock clock) {
        this.scheduler = new SchedulerEngine(settings, clock);
        this.clusterService = clusterService;
        this.snapshotTask = new SnapshotLifecycleTask(client);
        clusterService.addLocalNodeMasterListener(this); // TODO: change this not to use 'this'
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (this.isMaster) {
            // TODO: handle modified policies (currently they are ignored)
            // TODO: handle deleted policies
            scheduleSnapshotJobs(event.state());
        }
    }

    @Override
    public void onMaster() {
        this.isMaster = true;
        scheduler.register(snapshotTask);
        scheduleSnapshotJobs(clusterService.state());
    }

    @Override
    public void offMaster() {
        this.isMaster = false;
        scheduler.unregister(snapshotTask);
        cancelSnapshotJobs();
    }

    /**
     * Schedule all non-scheduled snapshot jobs contained in the cluster state
     */
    public void scheduleSnapshotJobs(final ClusterState state) {
        SnapshotLifecycleMetadata snapMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta != null) {
            snapMeta.getSnapshotConfigurations().values().forEach(this::maybeScheduleSnapshot);
        }
    }

    /**
     * Schedule the {@link SnapshotLifecyclePolicy} job if it does not already exist. If the job already
     * exists it is not interfered with.
     */
    public void maybeScheduleSnapshot(final SnapshotLifecyclePolicyMetadata snapshotLifecyclePolicy) {
        final String jobId = snapshotLifecyclePolicy.getPolicy().getId();
        scheduledTasks.computeIfAbsent(jobId, id -> {
            final SchedulerEngine.Job job = new SchedulerEngine.Job(jobId,
                new CronSchedule(snapshotLifecyclePolicy.getPolicy().getSchedule()));
            logger.info("scheduling snapshot lifecycle job [{}]", jobId);
            scheduler.add(job);
            return job;
        });
    }

    /**
     * Cancel all scheduled snapshot jobs
     */
    public void cancelSnapshotJobs() {
        scheduler.scheduledJobIds().forEach(scheduler::remove);
        scheduledTasks.clear();
    }

    /**
     * Cancel the given snapshot lifecycle id
     */
    public void cancelScheduledSnapshot(final String snapshotLifecycleId) {
        scheduledTasks.remove(snapshotLifecycleId);
        scheduler.remove(snapshotLifecycleId);
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
