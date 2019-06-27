/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The {@code SnapshotRetentionTask} is invoked by the scheduled job from the
 * {@link SnapshotRetentionService}. It is responsible for retrieving the snapshots for repositories
 * that have an SLM policy configured, and then deleting the snapshots that fall outside the
 * retention policy.
 */
public class SnapshotRetentionTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(SnapshotRetentionTask.class);
    private static final AtomicBoolean running = new AtomicBoolean(false);

    private final Client client;
    private final ClusterService clusterService;

    public SnapshotRetentionTask(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        assert event.getJobName().equals(SnapshotRetentionService.SLM_RETENTION_JOB_ID) :
            "expected id to be " + SnapshotRetentionService.SLM_RETENTION_JOB_ID + " but it was " + event.getJobName();
        if (running.compareAndSet(false, true)) {
            try {
                logger.info("starting SLM retention snapshot cleanup task");
                final ClusterState state = clusterService.state();

                // Find all SLM policies that have retention enabled
                final Map<String, SnapshotLifecyclePolicy> policiesWithRetention = getAllPoliciesWithRetentionEnabled(state);

                // For those policies (there may be more than one for the same repo),
                // return the repos that we need to get the snapshots for
                final Set<String> repositioriesToFetch = policiesWithRetention.values().stream()
                    .map(SnapshotLifecyclePolicy::getRepository)
                    .collect(Collectors.toSet());

                // Find all the snapshots that are past their retention date
                // TODO: include min/max snapshot count as a criteria for deletion also
                final List<SnapshotInfo> snapshotsToBeDeleted = getAllSnapshots(repositioriesToFetch).stream()
                    .filter(snapshot -> snapshotEligibleForDeletion(snapshot, policiesWithRetention))
                    .collect(Collectors.toList());

                // Finally, delete the snapshots that need to be deleted
                deleteSnapshots(snapshotsToBeDeleted);

            } finally {
                running.set(false);
            }
        } else {
            logger.debug("snapshot lifecycle retention task started, but a task is already running, skipping");
        }
    }

    static Map<String, SnapshotLifecyclePolicy> getAllPoliciesWithRetentionEnabled(final ClusterState state) {
        // TODO: fill me in
        return Collections.emptyMap();
    }

    static boolean snapshotEligibleForDeletion(SnapshotInfo snapshot, Map<String, SnapshotLifecyclePolicy> policies) {
        // TODO: fill me in
        return false;
    }

    List<SnapshotInfo> getAllSnapshots(Collection<String> repositories) {
        // TODO: fill me in
        return Collections.emptyList();
    }

    void deleteSnapshots(List<SnapshotInfo> snapshotsToDelete) {
        // TODO: fill me in
        logger.info("deleting {}", snapshotsToDelete);
    }
}
