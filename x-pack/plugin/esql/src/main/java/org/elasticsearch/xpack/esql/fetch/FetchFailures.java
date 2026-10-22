/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shards whose fetch failed in one query, on its coordinator. A query that allows partial results goes on without the
 * rows of those shards, and reports them like the shards that failed in the query phase: its cluster counts them as
 * failed and lists their failures.
 * <p>
 * Thread safe. The fetch operators of the query record failures on the threads that receive the responses, and the query
 * folds them into its cluster once its coordinator, its data nodes and its fetches are done.
 */
public final class FetchFailures {
    /**
     * Like the query phase, the response lists a few failures rather than one per shard.
     */
    static final int MAX_LISTED_FAILURES = 5;

    private final boolean allowPartialResults;
    /**
     * The first failure of each shard, in the order the shards failed. Guarded by {@code this}. The fetch reads the local
     * cluster only, so a shard id names one shard.
     */
    private final Map<ShardId, ShardSearchFailure> failedShards = new LinkedHashMap<>();

    public FetchFailures(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
    }

    /**
     * Whether the query goes on without the rows of a fetch that failed with {@code e}, rather than failing.
     * <p>
     * Cancellation, circuit breaker trips and security failures always fail the query. An {@link IndexNotFoundException}
     * doesn't, unlike in the query phase ({@link EsqlCCSUtils#canAllowPartial}): during the fetch it means that an index
     * was deleted after the query phase read it, which {@code _search} reports as a shard failure too.
     */
    public boolean tolerates(Exception e) {
        return allowPartialResults
            && ExceptionsHelper.unwrap(
                e,
                TaskCancelledException.class,
                CircuitBreakingException.class,
                ElasticsearchSecurityException.class
            ) == null;
    }

    /**
     * Records that the fetch of {@code shardId} from {@code nodeId} failed with {@code e}. A shard counts once, with its first
     * failure.
     */
    public void shardFailed(String clusterAlias, String nodeId, ShardId shardId, Exception e) {
        ShardSearchFailure failure = new ShardSearchFailure(unwrap(e), new SearchShardTarget(nodeId, shardId, clusterAlias));
        synchronized (this) {
            failedShards.putIfAbsent(shardId, failure);
        }
    }

    /**
     * Moves the shards whose fetch failed from the successful to the failed shards of {@code cluster}, the local cluster of
     * the query, and lists the failures of the first ones, once per distinct failure, so a node that failed shows once.
     *
     * @return whether any fetch failed
     */
    public boolean applyTo(EsqlExecutionInfo.Cluster.Builder builder, EsqlExecutionInfo.Cluster cluster) {
        int failed;
        List<ShardSearchFailure> listed = new ArrayList<>();
        synchronized (this) {
            failed = failedShards.size();
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            for (ShardSearchFailure failure : failedShards.values()) {
                if (listed.size() < MAX_LISTED_FAILURES && seen.add(failure.getCause())) {
                    listed.add(failure);
                }
            }
        }
        if (failed == 0) {
            return false;
        }
        builder.setSuccessfulShards(Math.max(0, zeroIfNull(cluster.getSuccessfulShards()) - failed))
            .setFailedShards(zeroIfNull(cluster.getFailedShards()) + failed)
            .addFailures(listed);
        return true;
    }

    /**
     * The failure a remote node reported, rather than the transport exception that carried it.
     */
    static Exception unwrap(Exception e) {
        return e instanceof TransportException te ? FailureCollector.unwrapTransportException(te) : e;
    }

    private static int zeroIfNull(Integer value) {
        return value == null ? 0 : value;
    }
}
