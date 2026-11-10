/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Reports one native sequence snapshot's shard contribution through ordinary ES|QL execution metadata. All stages,
 * pages and payload fetches share that snapshot, so each response replaces its contribution instead of adding another
 * search's shard count. Each operator owns its own tracker; the execution info combines concurrent operator contributions.
 */
public final class SequenceExecutionInfo {
    @Nullable
    private final EsqlExecutionInfo executionInfo;
    private int total;
    private int successful;
    private int skipped;
    private int failed;
    private int openSkipped;
    private boolean terminalFailure;

    /** Allows local planner tests without a query execution-info object to use the same source implementation. */
    public SequenceExecutionInfo(@Nullable EsqlExecutionInfo executionInfo) {
        this.executionInfo = executionInfo;
    }

    /** Accounts for every shard whose snapshot was opened, even if early termination prevents further page reads. */
    public synchronized void onOpen(OpenPointInTimeResponse response) {
        if (executionInfo == null || terminalFailure) {
            return;
        }
        openSkipped = response.getSkippedShards();
        update(response.getTotalShards(), response.getSuccessfulShards(), openSkipped, response.getFailedShards());
        if (response.getFailedShards() != 0) {
            fail(List.of(new ShardSearchFailure(new IllegalStateException("SEQUENCE requires a complete snapshot"))));
        }
    }

    /** Records partial-response failures, which sequence matching rejects, without counting each page's shards again. */
    public synchronized void onSearch(SearchResponse response) {
        if (executionInfo == null || terminalFailure) {
            return;
        }
        // PIT can-match omits these shards from the token. Page counts only cover retained slots, but the query's
        // source accounting must continue to include the successfully skipped portion of its original scope.
        update(
            response.getTotalShards() + openSkipped,
            response.getSuccessfulShards() + openSkipped,
            response.getSkippedShards() + openSkipped,
            response.getFailedShards()
        );
        if (response.isTimedOut() || response.getFailedShards() != 0) {
            if (response.isTimedOut()) {
                executionInfo.markPartial();
            }
            List<ShardSearchFailure> failures = Arrays.asList(response.getShardFailures());
            if (failures.isEmpty()) {
                failures = List.of(new ShardSearchFailure(new IllegalStateException("SEQUENCE source search timed out")));
            }
            fail(failures);
        }
    }

    /** Records a live query failure once; callbacks discarded after successful early termination must not call this. */
    public synchronized void onFailure(Exception failure) {
        if (executionInfo == null || terminalFailure) {
            return;
        }
        if (failure instanceof SearchPhaseExecutionException searchFailure && searchFailure.shardFailures().length > 0) {
            List<ShardSearchFailure> failures = Arrays.asList(searchFailure.shardFailures());
            if (total > 0) {
                int failedShards = Math.min(total - openSkipped, Math.max(failed, failures.size()));
                int successfulShards = total - failedShards;
                update(total, successfulShards, Math.min(skipped, successfulShards), failedShards);
            }
            fail(failures);
        } else {
            // Coordinator cancellation, rejection and conversion errors have no attributable failed shard. Preserve
            // the known shard counts instead of claiming every shard failed, and report the query's failure separately.
            fail(List.of(new ShardSearchFailure(failure)));
        }
    }

    private void update(int totalShards, int successfulShards, int skippedShards, int failedShards) {
        executionInfo.addCoordinatorShardCounts(
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            totalShards - total,
            successfulShards - successful,
            skippedShards - skipped,
            failedShards - failed
        );
        total = totalShards;
        successful = successfulShards;
        skipped = skippedShards;
        failed = failedShards;
    }

    private void fail(List<ShardSearchFailure> failures) {
        executionInfo.swapCluster(
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            (alias, cluster) -> cluster == null
                ? null
                : new EsqlExecutionInfo.Cluster.Builder(cluster).setStatus(EsqlExecutionInfo.Cluster.Status.FAILED)
                    .addFailures(failures)
                    .build()
        );
        terminalFailure = true;
    }
}
