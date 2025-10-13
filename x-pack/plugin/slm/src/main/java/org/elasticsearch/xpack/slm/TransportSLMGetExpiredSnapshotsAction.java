/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Computes the expired snapshots for SLM. Called by {@link SnapshotRetentionTask}, but made into a separate (local-only) transport action
 * so that it can access the {@link RepositoriesService} directly.
 */
public class TransportSLMGetExpiredSnapshotsAction extends TransportAction<
    TransportSLMGetExpiredSnapshotsAction.Request,
    TransportSLMGetExpiredSnapshotsAction.Response> {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:admin/slm/execute/get_expired_snapshots");

    private static final Logger logger = LogManager.getLogger(TransportSLMGetExpiredSnapshotsAction.class);

    private final RepositoriesService repositoriesService;
    private final Executor retentionExecutor;

    @Inject
    public TransportSLMGetExpiredSnapshotsAction(
        TransportService transportService,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters
    ) {
        super(INSTANCE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.repositoriesService = repositoriesService;
        this.retentionExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
    }

    private static class ResultsBuilder {
        private final Map<String, List<Tuple<SnapshotId, String>>> resultsByRepository = ConcurrentCollections.newConcurrentMap();

        Response getResponse() {
            // copyOf just so we aren't returning the CHM
            return new Response(Map.copyOf(resultsByRepository));
        }

        void addResult(String repository, List<Tuple<SnapshotId, String>> snapshotsToDelete) {
            // snapshotsToDelete is immutable because it comes from a Stream#toList() so no further copying needed
            if (snapshotsToDelete.isEmpty()) {
                assert resultsByRepository.containsKey(repository) == false;
            } else {
                final var previousValue = resultsByRepository.put(repository, snapshotsToDelete);
                assert previousValue == null : repository + ": " + previousValue + " vs " + snapshotsToDelete;
            }
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var resultsBuilder = new ResultsBuilder();
        try (var refs = new RefCountingRunnable(() -> listener.onResponse(resultsBuilder.getResponse()))) {
            for (final var repositoryName : request.repositories()) {

                final Repository repository;
                try {
                    repository = repositoriesService.repository(repositoryName);
                } catch (RepositoryMissingException e) {
                    logger.debug("[{}]: repository not found", repositoryName);
                    continue;
                }

                if (repository.isReadOnly()) {
                    logger.debug("[{}]: skipping readonly repository", repositoryName);
                    continue;
                }

                retentionExecutor.execute(ActionRunnable.wrap(ActionListener.releaseAfter(new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {}

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(Strings.format("[%s]: could not compute expired snapshots", repositoryName), e);
                    }
                }, refs.acquire()),
                    perRepositoryListener -> SubscribableListener

                        // Get repository data
                        .<RepositoryData>newForked(l -> repository.getRepositoryData(retentionExecutor, l))

                        // Collect snapshot details by policy, and get any missing details by reading SnapshotInfo
                        .<SnapshotDetailsByPolicy>andThen(
                            (l, repositoryData) -> getSnapshotDetailsByPolicy(retentionExecutor, repository, repositoryData, l)
                        )

                        // Compute snapshots to delete for each (relevant) policy
                        .andThenAccept(snapshotDetailsByPolicy -> {
                            resultsBuilder.addResult(
                                repositoryName,
                                getSnapshotsToDelete(repositoryName, request.policies(), snapshotDetailsByPolicy)
                            );
                        })

                        // And notify this repository's listener on completion
                        .addListener(perRepositoryListener)
                ));
            }
        }
    }

    static class SnapshotDetailsByPolicy {
        private final Map<String, Map<SnapshotId, RepositoryData.SnapshotDetails>> snapshotsByPolicy = new HashMap<>();

        synchronized void add(SnapshotId snapshotId, RepositoryData.SnapshotDetails snapshotDetails) {
            assert RETAINABLE_STATES.contains(snapshotDetails.getSnapshotState());
            final var slmPolicy = snapshotDetails.getSlmPolicy();
            if (Strings.hasLength(slmPolicy)) {
                snapshotsByPolicy.computeIfAbsent(slmPolicy, ignored -> new HashMap<>()).put(snapshotId, snapshotDetails);
            }
        }

        <T> Stream<T> flatMap(BiFunction<String, Map<SnapshotId, RepositoryData.SnapshotDetails>, Stream<T>> fn) {
            // no need for synchronized here, building the map entirely happens-before reading it
            return snapshotsByPolicy.entrySet().stream().flatMap(entry -> fn.apply(entry.getKey(), entry.getValue()));
        }
    }

    // Exposed for testing
    static void getSnapshotDetailsByPolicy(
        Executor executor,
        Repository repository,
        RepositoryData repositoryData,
        ActionListener<SnapshotDetailsByPolicy> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        final var snapshotDetailsByPolicy = new SnapshotDetailsByPolicy();
        final var snapshotsWithMissingDetails = new ArrayList<SnapshotId>();

        for (final var snapshotId : repositoryData.getSnapshotIds()) {
            if (repositoryData.hasMissingDetails(snapshotId)) {
                snapshotsWithMissingDetails.add(snapshotId);
            } else {
                snapshotDetailsByPolicy.add(snapshotId, Objects.requireNonNull(repositoryData.getSnapshotDetails(snapshotId)));
            }
        }

        if (snapshotsWithMissingDetails.isEmpty()) {
            listener.onResponse(snapshotDetailsByPolicy);
        } else {
            // rare bwc path, older repositories might not have snapshot details fully populated yet
            logger.debug(
                "[{}]: retrieving snapshot details from snapshot info for {}",
                repository.getMetadata().name(),
                snapshotsWithMissingDetails
            );
            repository.getSnapshotInfo(
                snapshotsWithMissingDetails,
                false,
                () -> false,
                snapshotInfo -> snapshotDetailsByPolicy.add(
                    snapshotInfo.snapshotId(),
                    RepositoryData.SnapshotDetails.fromSnapshotInfo(snapshotInfo)
                ),
                new ThreadedActionListener<>(executor, listener.map(ignored -> snapshotDetailsByPolicy))
            );
        }
    }

    // Exposed for testing
    static List<Tuple<SnapshotId, String>> getSnapshotsToDelete(
        String repositoryName,
        Map<String, SnapshotLifecyclePolicy> policies,
        SnapshotDetailsByPolicy snapshotDetailsByPolicy
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        return snapshotDetailsByPolicy.flatMap((policyName, snapshotsForPolicy) -> {
            final var policy = policies.get(policyName);
            if (policy == null) {
                // These snapshots were taken by a policy that doesn't exist, so cannot be deleted
                logger.debug("[{}]: unknown policy [{}]", repositoryName, policyName);
                return Stream.of();
            }

            final var retention = policy.getRetentionPolicy();
            if (retention == null || retention.equals(SnapshotRetentionConfiguration.EMPTY)) {
                // Retention is not configured in this policy, nothing to delete
                logger.debug("[{}]: policy [{}] has no retention configuration", repositoryName, policyName);
                return Stream.of();
            }

            if (Objects.equals(policy.getRepository(), repositoryName) == false) {
                // This policy applies to a different repository, nothing to delete
                logger.debug("[{}]: policy [{}] applies to repository [{}]", repositoryName, policyName, policy.getRepository());
                return Stream.of();
            }

            logger.trace("[{}]: policy [{}] covers [{}] snapshots", repositoryName, policyName, snapshotsForPolicy.size());
            return snapshotsForPolicy.entrySet().stream().filter(e -> {
                final var eligibleForDeletion = retention.isSnapshotEligibleForDeletion(e.getKey(), e.getValue(), snapshotsForPolicy);
                logger.debug(
                    "[{}]: testing snapshot [{}] deletion eligibility with policy [{}]: {}",
                    repositoryName,
                    e.getKey(),
                    policyName,
                    eligibleForDeletion ? "ELIGIBLE" : "INELIGIBLE"
                );
                return eligibleForDeletion;
            }).map(e -> Tuple.tuple(e.getKey(), policyName));
        }).toList();
    }

    public static final class Request extends LegacyActionRequest {
        private final Collection<String> repositories;
        private final Map<String, SnapshotLifecyclePolicy> policies;

        public Request(Collection<String> repositories, Map<String, SnapshotLifecyclePolicy> policies) {
            this.repositories = repositories;
            this.policies = policies;
        }

        public Collection<String> repositories() {
            return repositories;
        }

        public Map<String, SnapshotLifecyclePolicy> policies() {
            return policies;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }

    public static final class Response extends ActionResponse {
        private final Map<String, List<Tuple<SnapshotId, String>>> snapshotsToDelete;

        public Response(Map<String, List<Tuple<SnapshotId, String>>> snapshotsToDelete) {
            this.snapshotsToDelete = snapshotsToDelete;
        }

        public Map<String, List<Tuple<SnapshotId, String>>> snapshotsToDelete() {
            return snapshotsToDelete;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }

    private static final Set<SnapshotState> RETAINABLE_STATES = EnumSet.of(
        SnapshotState.SUCCESS,
        SnapshotState.FAILED,
        SnapshotState.PARTIAL
    );
}
