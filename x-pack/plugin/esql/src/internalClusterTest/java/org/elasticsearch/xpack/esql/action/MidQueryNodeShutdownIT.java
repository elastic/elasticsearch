/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Stops the data node that is executing a blocked search / ES|QL query, using the same hooks as a SIGTERM
 * ({@link ShutdownPrepareService#prepareForShutdown()} then close), while the query is coordinated elsewhere and
 * another copy of the shard exists.
 */
@TestLogging(
    value = "org.elasticsearch.node.ShutdownPrepareService:DEBUG,"
        + "org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler:DEBUG,"
        + "org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler:DEBUG",
    reason = "tracing what happens to in-flight work when a node is shut down"
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 1, supportsDedicatedMasters = false)
public class MidQueryNodeShutdownIT extends AbstractPausableIntegTestCase {

    /** How long the node is given to leave before the blocked scripts are released, standing in for "the query would finish soon". */
    private static final long LEAVE_WAIT_MILLIS = 5_000;

    private String coordinator;

    @Override
    protected Settings.Builder testIndexSettings() {
        coordinator = clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.canContainData() == false && node.isMasterNode() == false)
            .findFirst()
            .orElseThrow()
            .getName();
        return indexSettings(1, 1);
    }

    public void testSearchSurvivesDataNodeShutdown() throws Exception {
        runSearchAcrossDataNodeShutdown(true);
    }

    public void testSearchWithPartialResultsDisallowedSurvivesDataNodeShutdown() throws Exception {
        runSearchAcrossDataNodeShutdown(false);
    }

    private void runSearchAcrossDataNodeShutdown(boolean allowPartialResults) throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        ensureGreen("test");
        ActionFuture<SearchResponse> future = client(coordinator).prepareSearch("test")
            .setAllowPartialSearchResults(allowPartialResults)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("s").field("pause_me"))
            .execute();
        scriptWaits.acquire();
        String victim = nodeRunning("indices:data/read/search[*");
        logger.info("--> search is blocked on data node [{}]; shutting it down", victim);

        shutDownWhileBlocked(victim);

        SearchResponse response = future.actionGet();
        try {
            assertThat(((Sum) response.getAggregations().get("s")).value(), equalTo((double) numberOfDocs()));
        } finally {
            response.decRef();
        }
    }

    public void testSearchWithNoOtherCopyFailsWhenPartialResultsDisallowed() throws Exception {
        runSearchWithOnlyCopyOnShutdownNode(false);
    }

    public void testSearchWithNoOtherCopyWhenPartialResultsAllowed() throws Exception {
        runSearchWithOnlyCopyOnShutdownNode(true);
    }

    private void runSearchWithOnlyCopyOnShutdownNode(boolean allowPartialResults) throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 0), "test");
        ensureGreen("test");
        ActionFuture<SearchResponse> future = client(coordinator).prepareSearch("test")
            .setAllowPartialSearchResults(allowPartialResults)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("s").field("pause_me"))
            .execute();
        scriptWaits.acquire();
        String victim = nodeRunning("indices:data/read/search[*");
        logger.info(
            "--> search (allowPartialResults={}) is blocked on the only copy, on [{}]; shutting it down",
            allowPartialResults,
            victim
        );

        shutDownWhileBlocked(victim);

        try {
            SearchResponse response = future.actionGet();
            try {
                logger.info(
                    "--> search returned: failed shards [{}], successful [{}]",
                    response.getFailedShards(),
                    response.getSuccessfulShards()
                );
            } finally {
                response.decRef();
            }
        } catch (Exception e) {
            logger.info("--> search (allowPartialResults={}) failed: {}", allowPartialResults, e.toString());
        }
    }

    public void testEsqlSurvivesDataNodeShutdown() throws Exception {
        runEsqlAcrossDataNodeShutdown(false);
    }

    public void testEsqlWithPartialResultsAllowedSurvivesDataNodeShutdown() throws Exception {
        runEsqlAcrossDataNodeShutdown(true);
    }

    private void runEsqlAcrossDataNodeShutdown(boolean allowPartialResults) throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        ensureGreen("test");
        ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM test | STATS s = SUM(pause_me)").pragmas(randomPragmas()).allowPartialResults(allowPartialResults)
        );
        scriptWaits.acquire();
        String victim = nodeRunning("indices:data/read/esql/data");
        logger.info("--> esql is blocked on data node [{}]; shutting it down", victim);

        shutDownWhileBlocked(victim);

        try (EsqlQueryResponse response = future.actionGet()) {
            assertFalse(response.isPartial());
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
    }

    /**
     * Control: a running search coordinator task on the node being shut down must hold up the shutdown hooks.
     */
    public void testShutdownWaitsForRunningSearchOnCoordinator() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        ensureGreen("test");
        ActionFuture<SearchResponse> future = client(coordinator).prepareSearch("test")
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("s").field("pause_me"))
            .execute();
        scriptWaits.acquire();
        assertThat(nodeRunning("indices:data/read/search"), equalTo(coordinator));

        assertShutdownHooksWaitWhileQueryRuns(coordinator);

        future.actionGet().decRef();
    }

    /**
     * An async ES|QL query keeps running on its coordinator after the submit request has returned, like an async search does.
     */
    public void testShutdownWaitsForRunningAsyncEsqlOnCoordinator() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        ensureGreen("test");
        try (
            EsqlQueryResponse submitted = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                asyncEsqlQueryRequest("FROM test | STATS s = SUM(pause_me)").pragmas(randomPragmas())
            ).actionGet()
        ) {
            assertTrue("query should still be running after submit returned", submitted.isRunning());
        }
        scriptWaits.acquire();
        var tasks = client().admin().cluster().prepareListTasks().setActions("indices:data/read/esql*").setDetailed(true).get().getTasks();
        logger.info("--> esql tasks while async query is running: {}", tasks.stream().map(t -> t.node() + " " + t.action()).toList());
        assertThat(nodeRunning("indices:data/read/esql[a]"), equalTo(coordinator));

        assertShutdownHooksWaitWhileQueryRuns(coordinator);
    }

    private void assertShutdownHooksWaitWhileQueryRuns(String node) throws Exception {
        Thread prepare = new Thread(
            () -> internalCluster().getInstance(ShutdownPrepareService.class, node).prepareForShutdown(),
            "test-prepare-" + node
        );
        prepare.start();
        try {
            prepare.join(3_000);
            assertTrue("prepareForShutdown returned while a query was still running on [" + node + "]", prepare.isAlive());
        } finally {
            scriptPermits.release(Integer.MAX_VALUE / 2);
            prepare.join(60_000);
        }
    }

    /**
     * End to end: an async ES|QL query is submitted, its coordinator is shut down while it is still running, and the result is then
     * fetched by id from another node. An async search would have held the shutdown until it finished and stored its result.
     */
    public void testAsyncEsqlResultIsAvailableAfterCoordinatorShutdown() throws Exception {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        ensureGreen("test");
        EsqlQueryRequest request = asyncEsqlQueryRequest("FROM test | STATS s = SUM(pause_me)").pragmas(randomPragmas());
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(100));
        request.keepOnCompletion(true);
        String id;
        try (EsqlQueryResponse submitted = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
            assertTrue("query should still be running after submit returned", submitted.isRunning());
            id = submitted.asyncExecutionId().orElseThrow();
        }
        scriptWaits.acquire();
        String coordinatorNodeId = clusterService().state().nodes().resolveNode(coordinator).getId();
        logger.info("--> async query [{}] is running on coordinator [{}]; shutting the coordinator down", id, coordinator);

        shutDownWhileBlocked(coordinator);

        String survivor = survivor(coordinator);
        String coordinatorId = coordinatorNodeId;
        awaitClusterState(survivor, state -> state.nodes().nodeExists(coordinatorId) == false);
        var getRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        try (EsqlQueryResponse response = client(survivor).execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
            assertFalse("async query should have finished", response.isRunning());
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) numberOfDocs()))));
        }
    }

    private String survivor(String victim) {
        return internalCluster().getNodeNames()[0].equals(victim)
            ? internalCluster().getNodeNames()[1]
            : internalCluster().getNodeNames()[0];
    }

    private String nodeRunning(String actionPattern) throws Exception {
        AtomicReference<String> nodeId = new AtomicReference<>();
        assertBusy(() -> {
            var tasks = client().admin().cluster().prepareListTasks().setActions(actionPattern).get().getTasks();
            assertThat(tasks, not(empty()));
            nodeId.set(tasks.get(0).taskId().getNodeId());
        });
        return clusterService().state().nodes().get(nodeId.get()).getName();
    }

    /**
     * Runs the SIGTERM-style shutdown of {@code victim} on another thread and releases the blocked scripts once the node has left the
     * cluster, or after {@link #LEAVE_WAIT_MILLIS} if it is still waiting (as it would be if shutdown drained the work).
     */
    private void shutDownWhileBlocked(String victim) throws Exception {
        String victimId = clusterService().state().nodes().resolveNode(victim).getId();
        String observer = survivor(victim);
        Thread shutdown = new Thread(() -> {
            long start = System.nanoTime();
            internalCluster().getInstance(ShutdownPrepareService.class, victim).prepareForShutdown();
            logger.info("--> prepareForShutdown on [{}] returned after [{}]ms", victim, (System.nanoTime() - start) / 1_000_000);
            try {
                internalCluster().stopNode(victim);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, "test-shutdown-" + victim);
        shutdown.start();
        try {
            long deadline = System.currentTimeMillis() + LEAVE_WAIT_MILLIS;
            while (System.currentTimeMillis() < deadline
                && internalCluster().clusterService(observer).state().nodes().nodeExists(victimId)) {
                Thread.sleep(50);
            }
            logger.info(
                "--> releasing scripts; node [{}] still in cluster state: [{}]",
                victim,
                internalCluster().clusterService(observer).state().nodes().nodeExists(victimId)
            );
        } finally {
            scriptPermits.release(Integer.MAX_VALUE / 2);
        }
        shutdown.join(60_000);
    }
}
