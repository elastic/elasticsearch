/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQueryWithPragmas;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.waitForCluster;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterAsyncQueryStopIT extends AbstractCrossClusterTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CrossClusterAsyncQueryStopIT.class);

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testStopQuery() throws Exception {
        assumeTrue("Pragma does not work in release builds", Build.current().isSnapshot());
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        // Create large index so we could be sure we're stopping before the end
        populateRuntimeIndex(REMOTE_CLUSTER_2, "pause_count", INDEX_WITH_BLOCKING_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQueryWithPragmas(
            client(),
            "FROM logs-*,cluster-a:logs-*,remote-b:blocking | STATS total=sum(coalesce(const,v)) | LIMIT 1",
            includeCCSMetadata.v1(),
            Map.of("page_size", 1, "data_partitioning", "shard", "task_concurrency", 1)
        );
        try {
            // wait until we know that the query against 'remote-b:blocking' has started
            CountingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

            // wait until the query of 'cluster-a:logs-*' has finished (it is not blocked since we are not searching the 'blocking' index on
            // it)
            waitForCluster(client(), REMOTE_CLUSTER_1, asyncExecutionId);
            waitForCluster(client(), LOCAL_CLUSTER, asyncExecutionId);

            /* at this point:
             *  the query against cluster-a should be finished
             *  the query against remote-b should be running (blocked on the PauseFieldPlugin.allowEmitting CountDown)
             *  the query against the local cluster should be running because it has a STATS clause that needs to wait on remote-b
             */

            // run the stop query
            AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId);
            ActionFuture<EsqlQueryResponse> stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
            assertBusy(() -> {
                List<TaskInfo> tasks = getDriverTasks(client(REMOTE_CLUSTER_2));
                List<TaskInfo> reduceTasks = tasks.stream()
                    .filter(t -> t.description().contains("_LuceneSourceOperator") == false)
                    .toList();
                assertThat(reduceTasks, empty());
            });
            // allow remoteB query to proceed
            CountingPauseFieldPlugin.allowEmitting.countDown();

            // Since part of the query has not been stopped, we expect some result to emerge here
            try (EsqlQueryResponse asyncResponse = stopAction.actionGet(30, TimeUnit.SECONDS)) {
                // Check that we did not process all the fields on remote-b
                // Should not be getting more than one page here, and we set page size to 1
                assertThat(CountingPauseFieldPlugin.count.get(), lessThanOrEqualTo(1L));
                assertThat(asyncResponse.isRunning(), is(false));
                assertThat(asyncResponse.columns().size(), equalTo(1));
                assertThat(asyncResponse.values().hasNext(), is(true));
                Iterator<Object> row = asyncResponse.values().next();
                // sum of 0-9 is 45, and sum of 0-9 squared is 285
                assertThat(row.next(), equalTo(330L));

                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                assertThat(executionInfo.isPartial(), equalTo(true));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
                assertClusterInfoSuccess(remoteCluster, remote1NumShards);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("blocking"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
                assertClusterInfoSuccess(localCluster, localNumShards);

                assertClusterMetadataInResponse(asyncResponse, responseExpectMeta, 3);
            }
        } finally {
            // Ensure proper cleanup if the test fails
            CountingPauseFieldPlugin.allowEmitting.countDown();
            deleteAsyncId(client(), asyncExecutionId);
        }
    }

    public void testStopQueryLocal() throws Exception {
        assumeTrue("Pragma does not work in release builds", Build.current().isSnapshot());
        Map<String, Object> testClusterInfo = setupClusters(3);
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        populateRuntimeIndex(LOCAL_CLUSTER, "pause", INDEX_WITH_BLOCKING_MAPPING);

        // Gets random node client but ensure it's the same node for all operations
        Client client = cluster(LOCAL_CLUSTER).client();

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();
        // By default, ES|QL uses all workers in the esql_worker threadpool to execute drivers on data nodes.
        // If a node is both data and coordinator, and all drivers are blocked by the allowEmitting latch,
        // there are no workers left to execute the final driver or fetch pages from remote clusters.
        // This can prevent remote clusters from being marked as successful on the coordinator, even if they
        // have completed. To avoid this, we reserve at least one worker for the final driver and page fetching.
        // A single worker is enough, as these two tasks can be paused and yielded.
        var threadpool = cluster(LOCAL_CLUSTER).getInstance(TransportService.class).getThreadPool();
        int maxEsqlWorkers = threadpool.info(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME).getMax();
        LOGGER.info("--> Launching async query");
        final String asyncExecutionId = startAsyncQueryWithPragmas(
            client,
            "FROM blocking,*:logs-* | STATS total=sum(coalesce(const,v)) | LIMIT 1",
            includeCCSMetadata.v1(),
            Map.of(QueryPragmas.TASK_CONCURRENCY.getKey(), between(1, maxEsqlWorkers - 1))
        );
        try {
            // wait until we know that the local query against 'blocking' has started
            LOGGER.info("--> Waiting for {} to start", asyncExecutionId);
            assertTrue(SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));

            // wait until the remotes are done
            LOGGER.info("--> Waiting for remotes", asyncExecutionId);
            waitForCluster(client, REMOTE_CLUSTER_1, asyncExecutionId);
            LOGGER.info("--> Remote 1 done", asyncExecutionId);
            waitForCluster(client, REMOTE_CLUSTER_2, asyncExecutionId);
            LOGGER.info("--> Remote 2 done", asyncExecutionId);

            /* at this point:
             *  the query against remotes should be finished
             *  the query against the local cluster should be running because it's blocked
             */
            // run the stop query
            AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId);
            LOGGER.info("Launching stop for {}", asyncExecutionId);
            ActionFuture<EsqlQueryResponse> stopAction = client.execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
            // ensure stop operation is running
            assertBusy(() -> {
                try (EsqlQueryResponse asyncResponse = getAsyncResponse(client, asyncExecutionId)) {
                    EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                    LOGGER.info("--> Waiting for stop operation to start, current status: {}", executionInfo);
                    assertNotNull(executionInfo);
                    assertThat(executionInfo.isStopped(), is(true));
                }
            });
            // allow local query to proceed
            SimplePauseFieldPlugin.allowEmitting.countDown();
            LOGGER.info("--> Collecting results for {}", asyncExecutionId);

            // Since part of the query has not been stopped, we expect some result to emerge here
            try (EsqlQueryResponse asyncResponse = stopAction.actionGet(30, TimeUnit.SECONDS)) {
                assertThat(asyncResponse.isRunning(), is(false));
                assertThat(asyncResponse.columns().size(), equalTo(1));
                assertThat(asyncResponse.values().hasNext(), is(true));
                Iterator<Object> row = asyncResponse.values().next();
                // sum of 0-9 squared is 285, from two remotes it's 570
                assertThat(row.next(), equalTo(570L));

                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                assertThat(executionInfo.isPartial(), equalTo(true));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
                assertClusterInfoSuccess(remoteCluster, remote1NumShards);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertClusterInfoSuccess(remote2Cluster, remote2NumShards);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("blocking"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));

                assertClusterMetadataInResponse(asyncResponse, responseExpectMeta, 3);
            }
        } finally {
            SimplePauseFieldPlugin.allowEmitting.countDown();
            deleteAsyncId(client, asyncExecutionId);
        }
    }

    public void testStopQueryLocalNoRemotes() throws Exception {
        setupClusters(3);
        populateRuntimeIndex(LOCAL_CLUSTER, "pause", INDEX_WITH_BLOCKING_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();

        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM blocking | STATS total=count(const) | LIMIT 1",
            includeCCSMetadata.v1()
        );

        try {
            // wait until we know that the query against 'remote-b:blocking' has started
            SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

            /* at this point:
             *  the query against the local cluster should be running because it's blocked
             */

            // run the stop query
            var stopRequest = new AsyncStopRequest(asyncExecutionId);
            var stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
            // allow local query to proceed
            SimplePauseFieldPlugin.allowEmitting.countDown();

            try (EsqlQueryResponse asyncResponse = stopAction.actionGet(30, TimeUnit.SECONDS)) {
                assertThat(asyncResponse.isRunning(), is(false));
                assertThat(asyncResponse.columns().size(), equalTo(1));
                assertThat(asyncResponse.values().hasNext(), is(true));
                Iterator<Object> row = asyncResponse.values().next();
                assertThat((long) row.next(), greaterThanOrEqualTo(0L));

                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
            }
        } finally {
            SimplePauseFieldPlugin.allowEmitting.countDown();
            deleteAsyncId(client(), asyncExecutionId);
        }
    }

    // What happens here is when we run stop on subquery, the main query will still finish.
    // TODO: should we have a way to stop the main query as well?
    public void testStopQueryInlineStats() throws Exception {
        assumeTrue("Pragma does not work in release builds", Build.current().isSnapshot());
        Map<String, Object> testClusterInfo = setupClusters(3);
        // Create large index so we could be sure we're stopping before the end
        populateRuntimeIndex(REMOTE_CLUSTER_2, "pause_count", INDEX_WITH_BLOCKING_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQueryWithPragmas(
            client(),
            "FROM logs-*,remote-b:blocking | INLINE STATS total=sum(coalesce(const,v)) | KEEP total,foo | LIMIT 100",
            includeCCSMetadata.v1(),
            Map.of("page_size", 1, "data_partitioning", "shard", "task_concurrency", 1)
        );
        try {
            // wait until we know that the query against 'remote-b:blocking' has started
            CountingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

            /* at this point:
             *  the query against remote-b should be running (blocked on the PauseFieldPlugin.allowEmitting CountDown)
             *  the query against the local cluster should be running because it has a STATS clause that needs to wait on remote-b
             */

            // run the stop query
            AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId);
            ActionFuture<EsqlQueryResponse> stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
            assertBusy(() -> {
                List<TaskInfo> tasks = getDriverTasks(client(REMOTE_CLUSTER_2));
                List<TaskInfo> reduceTasks = tasks.stream()
                    .filter(t -> t.description().contains("_LuceneSourceOperator") == false)
                    .toList();
                assertThat(reduceTasks, empty());
            });
            // allow remoteB query to proceed
            CountingPauseFieldPlugin.allowEmitting.countDown();

            // We're not getting any proper result here since we stopped the query before it could complete
            try (EsqlQueryResponse asyncResponse = stopAction.actionGet(30, TimeUnit.SECONDS)) {
                assertThat(asyncResponse.isRunning(), is(false));
                assertThat(asyncResponse.columns().size(), equalTo(2));
                AtomicInteger i = new AtomicInteger(0);
                asyncResponse.values().forEachRemaining(row -> {
                    var v = row.next();
                    // The sum could be null, if the stats did not manage to compute anything before being stopped
                    // Or it could be 45L if it managed to add 0-9
                    if (v != null) {
                        assertThat((long) v, lessThanOrEqualTo(45L));
                    }
                    v = row.next();
                    if (v != null) {
                        assertThat((long) v, lessThanOrEqualTo(10L));
                    }
                    assertFalse(row.hasNext());
                    i.getAndIncrement();
                });
                // 20 is 10 rows from local and 10 rows from remote-b
                assertThat(i.get(), equalTo(20));

                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_2)));
                assertThat(executionInfo.isPartial(), equalTo(true));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("blocking"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));

                assertClusterMetadataInResponse(asyncResponse, responseExpectMeta, 2);
            }
        } finally {
            // Ensure proper cleanup if the test fails
            CountingPauseFieldPlugin.allowEmitting.countDown();
            deleteAsyncId(client(), asyncExecutionId);
        }
    }

    public void deleteAsyncId(Client client, String id) {
        try {
            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            assertAcked(client.execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(30, TimeUnit.SECONDS));
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.warn("timeout waiting for DELETE response: {}: {}", id, e);
        }
    }

}
