/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Which failed fetches a query goes on without, and how it reports the shards of those fetches.
 */
public class FetchFailuresTests extends ESTestCase {
    private static final String LOCAL = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    private static final Index INDEX = new Index("idx", "idx-uuid");

    /**
     * Without partial results, every failed fetch fails the query, also one whose context is gone.
     */
    public void testWithoutPartialResultsNothingIsTolerated() {
        FetchFailures failures = new FetchFailures(false);
        List<Exception> all = new ArrayList<>(shardFailures());
        all.add(new IndexNotFoundException("idx"));
        all.addAll(queryFailures());
        for (Exception e : all) {
            assertFalse(e.toString(), failures.tolerates(e));
        }
    }

    /**
     * With partial results, the query goes on without the failures of shards and of requests to a node: a missing context,
     * a failure while loading, a rejected request, a node that left.
     */
    public void testShardFailuresAreToleratedWithPartialResults() {
        FetchFailures failures = new FetchFailures(true);
        for (Exception e : shardFailures()) {
            assertTrue(e.toString(), failures.tolerates(e));
        }
    }

    /**
     * An index deleted between the query phase and the fetch is a shard failure, unlike a missing index in the query phase.
     */
    public void testAMissingIndexIsToleratedUnlikeInTheQueryPhase() {
        IndexNotFoundException e = new IndexNotFoundException("idx");
        assertFalse("the query phase fails on a missing index", EsqlCCSUtils.canAllowPartial(e));
        assertTrue(new FetchFailures(true).tolerates(e));
        assertTrue(new FetchFailures(true).tolerates(remote(e)));
    }

    /**
     * A cancellation, a circuit breaker trip or a security failure always fails the query, also when a transport exception
     * carries it.
     */
    public void testCancellationCircuitBreakingAndSecurityAreNeverTolerated() {
        FetchFailures failures = new FetchFailures(true);
        for (Exception e : queryFailures()) {
            assertFalse(e.toString(), failures.tolerates(e));
            assertFalse(e.toString(), failures.tolerates(remote(e)));
        }
    }

    /**
     * A shard that fails several times counts once, with its first failure and the node that reported it.
     */
    public void testAShardCountsOnceWithItsFirstFailure() {
        FetchFailures failures = new FetchFailures(true);
        Exception first = new SearchContextMissingException(new ShardSearchContextId("session", 1));
        failures.shardFailed(LOCAL, "n1", shard(0), first);
        failures.shardFailed(LOCAL, "n2", shard(0), new ElasticsearchException("second"));

        EsqlExecutionInfo.Cluster applied = apply(failures, cluster(3, 3, 0, List.of()));
        assertThat(applied.getFailedShards(), equalTo(1));
        assertThat(applied.getFailures(), hasSize(1));
        ShardSearchFailure failure = applied.getFailures().getFirst();
        assertThat(failure.getCause(), sameInstance(first));
        assertThat(failure.shard(), equalTo(new SearchShardTarget("n1", shard(0), LOCAL)));
    }

    /**
     * A failure that a transport exception carries is recorded as the failure the remote node reported.
     */
    public void testRecordsTheFailureATransportExceptionCarries() {
        FetchFailures failures = new FetchFailures(true);
        Exception reported = new SearchContextMissingException(new ShardSearchContextId("session", 1));
        failures.shardFailed(LOCAL, "n1", shard(0), new TransportException("failed on [n1]", reported));

        EsqlExecutionInfo.Cluster applied = apply(failures, cluster(1, 1, 0, List.of()));
        assertThat(applied.getFailures().getFirst().getCause(), sameInstance(reported));
    }

    /**
     * Applying moves the shards whose fetch failed from the successful to the failed shards of the cluster, and keeps the
     * total and the failures of the query phase.
     */
    public void testApplyMovesTheFailedShardsToTheFailedOnes() {
        FetchFailures failures = new FetchFailures(true);
        failures.shardFailed(LOCAL, "n1", shard(1), new ElasticsearchException("one"));
        failures.shardFailed(LOCAL, "n2", shard(2), new ElasticsearchException("two"));
        ShardSearchFailure queryPhaseFailure = new ShardSearchFailure(
            new ElasticsearchException("query phase"),
            new SearchShardTarget("n3", shard(4), LOCAL)
        );

        EsqlExecutionInfo.Cluster applied = apply(failures, cluster(5, 4, 1, List.of(queryPhaseFailure)));
        assertThat(applied.getTotalShards(), equalTo(5));
        assertThat(applied.getSuccessfulShards(), equalTo(2));
        assertThat(applied.getSkippedShards(), equalTo(0));
        assertThat(applied.getFailedShards(), equalTo(3));
        assertThat(applied.getFailures(), hasSize(3));
        assertThat(applied.getFailures().getFirst(), sameInstance(queryPhaseFailure));
    }

    /**
     * Applying lists one failure per distinct exception, at most five, so a node whose request failed shows once.
     */
    public void testApplyListsOneFailurePerException() {
        FetchFailures failures = new FetchFailures(true);
        Exception nodeFailure = new EsRejectedExecutionException("rejected");
        int shardsOfTheNode = between(2, 4);
        for (int s = 0; s < shardsOfTheNode; s++) {
            failures.shardFailed(LOCAL, "n1", shard(s), nodeFailure);
        }
        List<Throwable> shardFailures = new ArrayList<>();
        int otherShards = FetchFailures.MAX_LISTED_FAILURES + between(0, 3);
        for (int s = 0; s < otherShards; s++) {
            Exception e = new ElasticsearchException("shard " + s);
            shardFailures.add(e);
            failures.shardFailed(LOCAL, "n2", shard(shardsOfTheNode + s), e);
        }
        int total = shardsOfTheNode + otherShards;

        EsqlExecutionInfo.Cluster applied = apply(failures, cluster(total, total, 0, List.of()));
        assertThat(applied.getFailedShards(), equalTo(total));
        List<Throwable> expected = new ArrayList<>();
        expected.add(nodeFailure);
        expected.addAll(shardFailures.subList(0, FetchFailures.MAX_LISTED_FAILURES - 1));
        assertThat(applied.getFailures().stream().map(ShardSearchFailure::getCause).toList(), equalTo(expected));
    }

    /**
     * Without failed fetches, applying changes nothing and reports that no fetch failed.
     */
    public void testApplyWithoutFailuresChangesNothing() {
        FetchFailures failures = new FetchFailures(true);
        ShardSearchFailure queryPhaseFailure = new ShardSearchFailure(
            new ElasticsearchException("query phase"),
            new SearchShardTarget("n1", shard(0), LOCAL)
        );
        EsqlExecutionInfo.Cluster cluster = cluster(3, 2, 1, List.of(queryPhaseFailure));
        EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(cluster);

        assertFalse(failures.applyTo(builder, cluster));
        EsqlExecutionInfo.Cluster built = builder.build();
        assertThat(built, equalTo(cluster));
        assertThat(built.getFailures(), equalTo(cluster.getFailures()));
    }

    /**
     * The fetch operators of a query record failures from several threads at once. Every failed shard counts, and a shard
     * that several threads fail counts once.
     */
    public void testRecordsFromSeveralThreads() {
        FetchFailures failures = new FetchFailures(true);
        int threads = between(2, 8);
        int shardsPerThread = between(10, 100);
        int sharedShards = between(1, 10);
        int ownShards = threads * shardsPerThread;
        startInParallel(threads, t -> {
            for (int s = 0; s < shardsPerThread; s++) {
                failures.shardFailed(LOCAL, "n" + t, shard(t * shardsPerThread + s), new ElasticsearchException("own"));
            }
            for (int s = 0; s < sharedShards; s++) {
                failures.shardFailed(LOCAL, "n" + t, shard(ownShards + s), new ElasticsearchException("shared"));
            }
        });
        int total = ownShards + sharedShards;

        EsqlExecutionInfo.Cluster applied = apply(failures, cluster(total, total, 0, List.of()));
        assertThat(applied.getSuccessfulShards(), equalTo(0));
        assertThat(applied.getFailedShards(), equalTo(total));
    }

    /**
     * Failures of one shard or of one request to a node, which a query with partial results goes on without.
     */
    private static List<Exception> shardFailures() {
        return List.of(
            new SearchContextMissingException(new ShardSearchContextId("session", 1)),
            remote(new SearchContextMissingException(new ShardSearchContextId("session", 2))),
            new ElasticsearchException("failed to load [message]"),
            new EsRejectedExecutionException("rejected"),
            new NoNodeAvailableException("node [n1] left the cluster")
        );
    }

    /**
     * Failures that fail the query whatever shard they come from.
     */
    private static List<Exception> queryFailures() {
        return List.of(
            new TaskCancelledException("cancelled"),
            new CircuitBreakingException("over the limit", CircuitBreaker.Durability.TRANSIENT),
            new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN)
        );
    }

    private static RemoteTransportException remote(Exception e) {
        return new RemoteTransportException("[n1][127.0.0.1:9300][indices:data/read/esql/fetch]", e);
    }

    private static ShardId shard(int id) {
        return new ShardId(INDEX, id);
    }

    /**
     * The local cluster as the data nodes leave it.
     */
    private static EsqlExecutionInfo.Cluster cluster(int total, int successful, int failed, List<ShardSearchFailure> failures) {
        return new EsqlExecutionInfo.Cluster(
            LOCAL,
            null,
            "idx",
            false,
            EsqlExecutionInfo.Cluster.Status.RUNNING,
            total,
            successful,
            0,
            failed,
            failures,
            null
        );
    }

    private static EsqlExecutionInfo.Cluster apply(FetchFailures failures, EsqlExecutionInfo.Cluster cluster) {
        EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(cluster);
        assertTrue("a fetch failed", failures.applyTo(builder, cluster));
        return builder.build();
    }
}
