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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The shards whose fetch failed in one query, on its coordinator. A query goes on without the rows of those shards when it
 * allows partial results, or when their cluster is a remote cluster that the query skips when it fails. It reports them
 * like the shards that failed in the query phase: their cluster counts them as failed and lists their failures.
 * <p>
 * Thread safe. The fetch operators of the query record failures on the threads that receive the responses, and the query
 * folds them into each cluster once its coordinator, its data nodes and its fetches are done.
 */
public final class FetchFailures {
    /**
     * Like the query phase, the response lists a few failures rather than one per shard.
     */
    static final int MAX_LISTED_FAILURES = 5;

    private final boolean allowPartialResults;
    private final Predicate<String> skipsClusterOnFailure;
    /**
     * The first failure of each shard, in the order the shards failed. Guarded by {@code this}.
     */
    private final Map<ClusterShard, ShardSearchFailure> failedShards = new LinkedHashMap<>();

    /**
     * A shard of a cluster. Two clusters can each have a shard with the same id.
     */
    private record ClusterShard(String clusterAlias, ShardId shardId) {}

    /**
     * @param skipsClusterOnFailure whether the query skips a cluster, by alias, when it fails, like
     *                              {@link EsqlExecutionInfo#shouldSkipOnFailure}
     */
    public FetchFailures(boolean allowPartialResults, Predicate<String> skipsClusterOnFailure) {
        this.allowPartialResults = allowPartialResults;
        this.skipsClusterOnFailure = skipsClusterOnFailure;
    }

    /**
     * Whether the query goes on without the rows of a fetch from the cluster {@code clusterAlias} that failed with
     * {@code e}, rather than failing.
     * <p>
     * Cancellation, circuit breaker trips and security failures always fail the query. An {@link IndexNotFoundException}
     * doesn't, unlike in the query phase ({@link EsqlCCSUtils#canAllowPartial}): during the fetch it means that an index
     * was deleted after the query phase read it, which {@code _search} reports as a shard failure too.
     */
    public boolean tolerates(String clusterAlias, Exception e) {
        return (allowPartialResults || skipsClusterOnFailure.test(clusterAlias))
            && ExceptionsHelper.unwrap(
                e,
                TaskCancelledException.class,
                CircuitBreakingException.class,
                ElasticsearchSecurityException.class
            ) == null;
    }

    /**
     * Records that the fetch of {@code shardId} of the cluster {@code clusterAlias} from {@code nodeId} failed with
     * {@code e}. A shard counts once, with its first failure.
     */
    public void shardFailed(String clusterAlias, String nodeId, ShardId shardId, Exception e) {
        ShardSearchFailure failure = new ShardSearchFailure(unwrap(e), new SearchShardTarget(nodeId, shardId, clusterAlias));
        synchronized (this) {
            failedShards.putIfAbsent(new ClusterShard(clusterAlias, shardId), failure);
        }
    }

    /**
     * The aliases of the clusters with a shard whose fetch failed.
     */
    public synchronized Set<String> failedClusters() {
        Set<String> clusters = new LinkedHashSet<>();
        failedShards.keySet().forEach(shard -> clusters.add(shard.clusterAlias()));
        return clusters;
    }

    /**
     * Moves the shards of the cluster {@code clusterAlias} whose fetch failed from the successful to the failed shards of
     * {@code cluster}, and lists the failures of the first ones, once per distinct failure, so a node that failed shows once.
     *
     * @return whether any fetch from the cluster failed
     */
    public boolean applyTo(String clusterAlias, EsqlExecutionInfo.Cluster.Builder builder, EsqlExecutionInfo.Cluster cluster) {
        int failed = 0;
        List<ShardSearchFailure> listed = new ArrayList<>();
        synchronized (this) {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Map.Entry<ClusterShard, ShardSearchFailure> shard : failedShards.entrySet()) {
                if (shard.getKey().clusterAlias().equals(clusterAlias) == false) {
                    continue;
                }
                failed++;
                ShardSearchFailure failure = shard.getValue();
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
