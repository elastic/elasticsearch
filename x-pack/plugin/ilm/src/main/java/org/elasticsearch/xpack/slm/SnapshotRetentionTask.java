/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotRetentionConfiguration;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

                getAllSnapshots(repositioriesToFetch, new ActionListener<>() {
                    @Override
                    public void onResponse(List<Tuple<String, SnapshotInfo>> allSnapshots) {
                        // Find all the snapshots that are past their retention date
                        final List<Tuple<String, SnapshotInfo>> snapshotsToBeDeleted = allSnapshots.stream()
                            .filter(snapshot -> snapshotEligibleForDeletion(snapshot.v2(), allSnapshots, policiesWithRetention))
                            .collect(Collectors.toList());

                        // Finally, delete the snapshots that need to be deleted
                        deleteSnapshots(snapshotsToBeDeleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        running.set(false);
                    }
                }, err -> running.set(false));

            } finally {
                running.set(false);
            }
        } else {
            logger.trace("snapshot lifecycle retention task started, but a task is already running, skipping");
        }
    }

    static Map<String, SnapshotLifecyclePolicy> getAllPoliciesWithRetentionEnabled(final ClusterState state) {
        final SnapshotLifecycleMetadata snapMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null) {
            return Collections.emptyMap();
        }
        return snapMeta.getSnapshotConfigurations().entrySet().stream()
            .filter(e -> e.getValue().getPolicy().getRetentionPolicy() != null)
            .filter(e -> e.getValue().getPolicy().getRetentionPolicy().equals(SnapshotRetentionConfiguration.EMPTY) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPolicy()));
    }

    static boolean snapshotEligibleForDeletion(SnapshotInfo snapshot, List<Tuple<String, SnapshotInfo>> allSnapshots,
                                               Map<String, SnapshotLifecyclePolicy> policies) {
        if (snapshot.userMetadata() == null) {
            // This snapshot has no metadata, it is not eligible for deletion
            return false;
        }

        final String policyId;
        try {
            policyId = (String) snapshot.userMetadata().get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD);
        } catch (Exception e) {
            logger.error("unable to retrieve policy id from snapshot metadata [" + snapshot.userMetadata() + "]", e);
            throw e;
        }

        SnapshotLifecyclePolicy policy = policies.get(policyId);
        if (policy == null) {
            // This snapshot was taking by a policy that doesn't exist, so it's not eligible
            return false;
        }

        SnapshotRetentionConfiguration retention = policy.getRetentionPolicy();
        if (retention == null || retention.equals(SnapshotRetentionConfiguration.EMPTY)) {
            // Retention is not configured
            return false;
        }

        final String repository = policy.getRepository();
        // Retrieve the predicate based on the retention policy, passing in snapshots pertaining only to *this* policy and repository
        boolean eligible = retention.getSnapshotDeletionPredicate(
            allSnapshots.stream()
                .filter(t -> t.v1().equals(repository))
                .filter(t -> Optional.ofNullable(t.v2().userMetadata())
                    .map(meta -> meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD))
                    .map(pId -> pId.equals(policyId))
                    .orElse(false))
                .map(Tuple::v2).collect(Collectors.toList()))
            .test(snapshot);
        logger.debug("[{}] testing snapshot [{}] deletion eligibility: {}",
            repository, snapshot.snapshotId(), eligible ? "ELIGIBLE" : "INELIGIBLE");
        return eligible;
    }

    void getAllSnapshots(Collection<String> repositories, ActionListener<List<Tuple<String, SnapshotInfo>>> listener,
                         Consumer<Exception> errorHandler) {
        client.admin().cluster().prepareGetSnapshots(repositories.toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .execute(new ActionListener<GetSnapshotsResponse>() {
                @Override
                public void onResponse(final GetSnapshotsResponse resp) {
                    listener.onResponse(repositories.stream()
                        .flatMap(repo -> {
                            try {
                                return resp.getSnapshots(repo).stream()
                                    .map(si -> new Tuple<>(repo, si));
                            } catch (Exception e) {
                                logger.debug(new ParameterizedMessage("unable to retrieve snapshots for [{}] repository", repo), e);
                                return Stream.empty();
                            }
                        })
                        .collect(Collectors.toList()));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(new ParameterizedMessage("unable to retrieve snapshots for [{}] repositories", repositories), e);
                    errorHandler.accept(e);
                }
            });
    }

    void deleteSnapshots(List<Tuple<String, SnapshotInfo>> snapshotsToDelete) {
        // TODO: make this more resilient and possibly only delete for a certain amount of time
        logger.info("starting snapshot retention deletion for [{}] snapshots", snapshotsToDelete.size());
        snapshotsToDelete.forEach(snap -> {
            logger.info("[{}] snapshot retention deleting snapshot [{}]", snap.v1(), snap.v2().snapshotId());
            CountDownLatch latch = new CountDownLatch(1);
            client.admin().cluster().prepareDeleteSnapshot(snap.v1(), snap.v2().snapshotId().getName())
                .execute(new LatchedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        if (acknowledgedResponse.isAcknowledged()) {
                            logger.debug("[{}] snapshot [{}] deleted successfully", snap.v1(), snap.v2().snapshotId());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(new ParameterizedMessage("[{}] failed to delete snapshot [{}] for retention",
                            snap.v1(), snap.v2().snapshotId()), e);
                    }
                }, latch));
            try {
                // Deletes cannot occur simultaneously, so wait for this
                // deletion to complete before attempting the next one
                latch.await();
            } catch (InterruptedException e) {
                logger.error(new ParameterizedMessage("[{}] deletion of snapshot [{}] interrupted",
                    snap.v1(), snap.v2().snapshotId()), e);
            }
        });
    }
}
