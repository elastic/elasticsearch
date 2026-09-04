/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Defers per-producer shard counts and status so a source fan-in or MergeExec can merge
 * several index or remote outcomes and apply them to {@link EsqlExecutionInfo} once.
 * Session subplans (INLINE STATS, IN subquery, approximation) share that execution info
 * with the main plan, so status stays {@code RUNNING} until the main plan finishes.
 */
final class SourceOutcomeAccumulator {
    private final Map<SourceClusterKey, IndexProducerOutcome> indexOutcomes = new ConcurrentHashMap<>();
    private final List<Exception> externalSourceFailures = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, Long> remoteTookNanos = new ConcurrentHashMap<>();
    private final AtomicBoolean indexSourceSucceeded = new AtomicBoolean();
    private final AtomicBoolean externalSourceSucceeded = new AtomicBoolean();
    private final AtomicBoolean applied = new AtomicBoolean();

    void recordIndexResponse(SourceClusterKey key, ComputeResponse response) {
        record(key, IndexProducerOutcome.fromResponse(response));
    }

    void recordIndexFailure(SourceClusterKey key, Exception failure) {
        record(key, new IndexProducerOutcome(false, 0, 0, 0, 0, List.of(new ShardSearchFailure(failure)), true, false));
    }

    void recordRemoteOutcome(SourceClusterKey key, ClusterComputeHandler.RemoteClusterOutcome outcome) {
        switch (outcome) {
            case ClusterComputeHandler.RemoteClusterOutcome.Success success -> {
                recordIndexResponse(key, success.response());
                recordRemoteTook(key, success.response());
            }
            case ClusterComputeHandler.RemoteClusterOutcome.ToleratedFailure toleratedFailure -> {
                IndexProducerOutcome failure = new IndexProducerOutcome(
                    false,
                    0,
                    0,
                    0,
                    0,
                    List.of(new ShardSearchFailure(toleratedFailure.failure())),
                    toleratedFailure.status() == EsqlExecutionInfo.Cluster.Status.PARTIAL,
                    toleratedFailure.status() == EsqlExecutionInfo.Cluster.Status.SKIPPED
                );
                record(
                    key,
                    toleratedFailure.response() == null
                        ? failure
                        : IndexProducerOutcome.fromResponse(toleratedFailure.response()).merge(failure)
                );
                recordRemoteTook(key, toleratedFailure.response());
            }
            case ClusterComputeHandler.RemoteClusterOutcome.Skipped skipped -> record(
                key,
                new IndexProducerOutcome(
                    false,
                    0,
                    0,
                    0,
                    0,
                    List.of(),
                    skipped.status() == EsqlExecutionInfo.Cluster.Status.PARTIAL,
                    skipped.status() == EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );
        }
    }

    void recordExternalSuccess() {
        externalSourceSucceeded.set(true);
    }

    void recordExternalFailure(Exception failure) {
        externalSourceFailures.add(failure);
    }

    boolean externalSourceSucceeded() {
        return externalSourceSucceeded.get();
    }

    void failIfAllSourcesFailed(EsqlExecutionInfo execInfo, List<Page> finalResults) {
        if (externalSourceSucceeded() || indexSourceSucceeded.get()) {
            return;
        }
        FailureCollector failureCollector = new FailureCollector();
        for (IndexProducerOutcome outcome : indexOutcomes.values()) {
            for (ShardSearchFailure failure : outcome.failures()) {
                if (failure.getCause() instanceof Exception e) {
                    failureCollector.unwrapAndCollect(e);
                } else {
                    failureCollector.unwrapAndCollect(failure);
                }
            }
        }
        externalSourceFailures.forEach(failureCollector::unwrapAndCollect);
        ExceptionsHelper.reThrowIfNotNull(failureCollector.getFailure());
        ComputeService.failIfAllShardsFailed(execInfo, finalResults);
    }

    void applyTo(EsqlExecutionInfo execInfo) {
        if (applied.compareAndSet(false, true) == false) {
            return;
        }
        // INLINE STATS / IN subquery / approximation calibration run as a separate execute()
        // on the same EsqlExecutionInfo. Finalizing remotes here makes the main plan's
        // shouldSkipRemoteCluster treat them as already done and skip the only copy of the data.
        boolean finalizeStatus = execInfo.isMainPlan();
        Map<String, List<IndexProducerOutcome>> outcomesByCluster = new HashMap<>();
        indexOutcomes.forEach(
            (key, outcome) -> outcomesByCluster.computeIfAbsent(key.clusterAlias(), ignored -> new ArrayList<>()).add(outcome)
        );
        outcomesByCluster.forEach((clusterAlias, outcomes) -> execInfo.swapCluster(clusterAlias, (key, cluster) -> {
            if (cluster == null) {
                // A producer read a cluster that planning never registered, so there is no entry to merge into.
                // swapCluster would store this back under the alias, and a Cluster cannot be built without one.
                return null;
            }
            int totalShards = 0;
            int successfulShards = 0;
            int skippedShards = 0;
            int failedShards = 0;
            boolean hasResponse = false;
            boolean partial = execInfo.isStopped();
            boolean skippedFailure = false;
            List<ShardSearchFailure> failures = new ArrayList<>();
            for (IndexProducerOutcome outcome : outcomes) {
                totalShards += outcome.totalShards();
                successfulShards += outcome.successfulShards();
                skippedShards += outcome.skippedShards();
                failedShards += outcome.failedShards();
                hasResponse |= outcome.hasResponse();
                partial |= outcome.partial();
                skippedFailure |= outcome.skippedFailure();
                failures.addAll(outcome.failures());
            }
            Long remoteTook = remoteTookNanos.get(clusterAlias);
            TimeValue took = remoteTook == null
                ? execInfo.queryProfile().total().timeSinceStarted()
                : TimeValue.timeValueNanos(execInfo.queryProfile().planning().timeTook().nanos() + remoteTook);
            var builder = new EsqlExecutionInfo.Cluster.Builder(cluster).setTook(took).addFailures(failures);
            if (hasResponse || skippedFailure) {
                builder.setTotalShards(totalShards)
                    .setSuccessfulShards(successfulShards)
                    .setSkippedShards(skippedShards)
                    .setFailedShards(failedShards);
            }
            if (finalizeStatus && cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                if (skippedFailure && hasResponse == false && partial == false) {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SKIPPED);
                } else if (partial || failures.isEmpty() == false || skippedFailure) {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL);
                } else {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                }
            }
            return builder.build();
        }));
        if (finalizeStatus == false) {
            return;
        }
        for (String clusterAlias : execInfo.clusterAliases()) {
            execInfo.swapCluster(clusterAlias, (key, cluster) -> {
                if (cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                    return cluster;
                }
                // No producer recorded an outcome: LIMIT already filled the exchange, or the
                // producer was unused. CCS SKIPPED means skip-unavailable, not already finished.
                var builder = new EsqlExecutionInfo.Cluster.Builder(cluster).setTook(execInfo.queryProfile().total().timeSinceStarted())
                    .setStatus(
                        execInfo.isStopped() ? EsqlExecutionInfo.Cluster.Status.PARTIAL : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL
                    );
                return builder.build();
            });
        }
    }

    private void record(SourceClusterKey key, IndexProducerOutcome outcome) {
        if (outcome.successfulShards() > 0 || (outcome.hasResponse() && outcome.failedShards() == 0 && outcome.failures().isEmpty())) {
            indexSourceSucceeded.set(true);
        }
        indexOutcomes.merge(key, outcome, IndexProducerOutcome::merge);
    }

    private void recordRemoteTook(SourceClusterKey key, ComputeResponse response) {
        if (response != null && response.getTook() != null) {
            remoteTookNanos.merge(key.clusterAlias(), response.getTook().nanos(), Long::sum);
        }
    }

    /**
     * Identifies one index producer by the cluster it reads and the indices it was asked for. A cluster alias alone
     * is not enough: a fan-in can hold several producers against the same cluster, and their outcomes have to stay
     * apart until {@link #applyTo} merges them per cluster.
     */
    record SourceClusterKey(String clusterAlias, List<String> originalIndices) {
        static SourceClusterKey of(String clusterAlias, OriginalIndices originalIndices) {
            return new SourceClusterKey(clusterAlias, originalIndices == null ? List.of() : List.of(originalIndices.indices()));
        }
    }

    private record IndexProducerOutcome(
        boolean hasResponse,
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        List<ShardSearchFailure> failures,
        boolean partial,
        boolean skippedFailure
    ) {
        IndexProducerOutcome {
            failures = List.copyOf(failures);
        }

        static IndexProducerOutcome fromResponse(ComputeResponse response) {
            return new IndexProducerOutcome(
                true,
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getSkippedShards(),
                response.getFailedShards(),
                response.getFailures(),
                response.getFailedShards() > 0 || response.getFailures().isEmpty() == false,
                false
            );
        }

        IndexProducerOutcome merge(IndexProducerOutcome other) {
            IndexProducerOutcome counts = selectCounts(other);
            List<ShardSearchFailure> mergedFailures = new ArrayList<>(failures.size() + other.failures.size());
            mergedFailures.addAll(failures);
            mergedFailures.addAll(other.failures);
            return new IndexProducerOutcome(
                hasResponse || other.hasResponse,
                counts.totalShards,
                counts.successfulShards,
                counts.skippedShards,
                counts.failedShards,
                mergedFailures,
                partial || other.partial,
                skippedFailure || other.skippedFailure
            );
        }

        private IndexProducerOutcome selectCounts(IndexProducerOutcome other) {
            if (totalShards != other.totalShards) {
                return totalShards > other.totalShards ? this : other;
            }
            if (failedShards != other.failedShards) {
                return failedShards > other.failedShards ? this : other;
            }
            if (successfulShards != other.successfulShards) {
                return successfulShards < other.successfulShards ? this : other;
            }
            return skippedShards >= other.skippedShards ? this : other;
        }
    }
}
