/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
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
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    public void testStopQueryLocal() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        populateRuntimeIndex(LOCAL_CLUSTER, "pause", INDEX_WITH_BLOCKING_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM blocking,*:logs-* | STATS total=sum(coalesce(const,v)) | LIMIT 1",
            includeCCSMetadata.v1()
        );

        try {
            // wait until we know that the local query against 'blocking' has started
            assertTrue(SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));

            // wait until the remotes are done
            waitForCluster(client(), REMOTE_CLUSTER_1, asyncExecutionId);
            waitForCluster(client(), REMOTE_CLUSTER_2, asyncExecutionId);

            /* at this point:
             *  the query against remotes should be finished
             *  the query against the local cluster should be running because it's blocked
             */
            // run the stop query
            AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId);
            LOGGER.info("Launching stop for {}", asyncExecutionId);
            ActionFuture<EsqlQueryResponse> stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
            // ensure stop operation is running
            assertBusy(() -> {
                try (EsqlQueryResponse asyncResponse = getAsyncResponse(client(), asyncExecutionId)) {
                    EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                    LOGGER.info("Waiting for stop operation to start, current status: {}", executionInfo);
                    assertNotNull(executionInfo);
                    assertThat(executionInfo.isStopped(), is(true));
                }
            });
            // allow local query to proceed
            SimplePauseFieldPlugin.allowEmitting.countDown();
            LOGGER.info("Collecting results for {}", asyncExecutionId);

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
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
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
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }
}
