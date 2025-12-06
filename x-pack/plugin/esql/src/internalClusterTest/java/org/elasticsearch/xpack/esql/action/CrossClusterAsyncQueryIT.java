/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.runAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.waitForCluster;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CrossClusterAsyncQueryIT extends AbstractCrossClusterTestCase {
    /**
     * Includes testing for CCS metadata in the GET /_query/async/:id response while the search is still running
     */
    public void testSuccessfulPathways() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        populateRuntimeIndex(REMOTE_CLUSTER_2, "pause", INDEX_WITH_BLOCKING_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM logs-*,cluster-a:logs-*,remote-b:blocking | STATS total=sum(const) | LIMIT 10",
            includeCCSMetadata.v1()
        );
        // wait until we know that the query against 'remote-b:blocking' has started
        SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

        // wait until the query of 'cluster-a:logs-*' has finished (it is not blocked since we are not searching the 'blocking' index on it)
        waitForCluster(client(), "cluster-a", asyncExecutionId);

        /* at this point:
         *  the query against cluster-a should be finished
         *  the query against remote-b should be running (blocked on the PauseFieldPlugin.allowEmitting CountDown)
         *  the query against the local cluster should be running because it has a STATS clause that needs to wait on remote-b
         */
        try (EsqlQueryResponse asyncResponse = getAsyncResponse(client(), asyncExecutionId)) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            assertThat(asyncResponse.isRunning(), is(true));
            assertThat(
                executionInfo.clusterAliases(),
                equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING).count(), equalTo(2L));
            assertThat(executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL).count(), equalTo(1L));

            EsqlExecutionInfo.Cluster clusterA = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // Should be done and successful
            assertClusterInfoSuccess(clusterA, clusterA.getTotalShards());

            EsqlExecutionInfo.Cluster local = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            // should still be RUNNING since the local cluster has to do a STATS on the coordinator, waiting on remoteB
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertThat(clusterA.getTotalShards(), greaterThanOrEqualTo(1));

            EsqlExecutionInfo.Cluster remoteB = executionInfo.getCluster(REMOTE_CLUSTER_2);
            // should still be RUNNING since we haven't released the countdown lock to proceed
            assertThat(remoteB.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remoteB.getSuccessfulShards());  // should not be filled in until query is finished

            assertClusterMetadataInResponse(asyncResponse, responseExpectMeta, 3);
        }

        // allow remoteB query to proceed
        SimplePauseFieldPlugin.allowEmitting.countDown();

        // wait until both remoteB and local queries have finished
        assertBusy(() -> {
            try (EsqlQueryResponse asyncResponse = getAsyncResponse(client(), asyncExecutionId)) {
                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster remoteB = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remoteB.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
                EsqlExecutionInfo.Cluster local = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                assertThat(local.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
                assertThat(asyncResponse.isRunning(), is(false));
            }
        });

        try (EsqlQueryResponse asyncResponse = getAsyncResponse(client(), asyncExecutionId)) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(1L));
            assertThat(executionInfo.isPartial(), equalTo(false));

            EsqlExecutionInfo.Cluster clusterA = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(clusterA, remote1NumShards);

            EsqlExecutionInfo.Cluster remoteB = executionInfo.getCluster(REMOTE_CLUSTER_2);
            assertClusterInfoSuccess(remoteB, 1);

            EsqlExecutionInfo.Cluster local = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertClusterInfoSuccess(local, localNumShards);

            // Check that stop produces the same result
            try (
                EsqlQueryResponse stopResponse = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(asyncExecutionId))
                    .get()
            ) {
                assertThat(stopResponse, equalTo(asyncResponse));
            }
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    public void testAsyncQueriesWithLimit0() throws IOException {
        setupClusters(3);
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final TimeValue waitForCompletion = TimeValue.timeValueNanos(randomFrom(1L, Long.MAX_VALUE));
        String asyncExecutionId = null;
        try (EsqlQueryResponse resp = runAsyncQuery(client(), "FROM logs*,*:logs* | LIMIT 0", requestIncludeMeta, waitForCompletion)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            if (resp.isRunning()) {
                asyncExecutionId = resp.asyncExecutionId().get();
                assertThat(resp.columns().size(), equalTo(0));
                assertThat(resp.values().hasNext(), is(false));  // values should be empty list

            } else {
                assertThat(resp.columns().size(), equalTo(4));
                assertThat(resp.columns(), hasItem(new ColumnInfoImpl("const", "long", null)));
                assertThat(resp.columns(), hasItem(new ColumnInfoImpl("id", "keyword", null)));
                assertThat(resp.columns(), hasItem(new ColumnInfoImpl("tag", "keyword", null)));
                assertThat(resp.columns(), hasItem(new ColumnInfoImpl("v", "long", null)));
                assertThat(resp.values().hasNext(), is(false));  // values should be empty list

                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                assertThat(executionInfo.isPartial(), equalTo(false));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertClusterInfoSuccess(remoteCluster, 0);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSuccess(remote2Cluster, 0);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, 0);
                assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                assertClusterMetadataInResponse(resp, responseExpectMeta, 3);
            }
        } finally {
            if (asyncExecutionId != null) {
                assertAcked(deleteAsyncId(client(), asyncExecutionId));
            }
        }
    }

    public void testAsyncFailure() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(2);
        populateRuntimeIndex(REMOTE_CLUSTER_1, "pause_fail", INDEX_WITH_FAIL_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM logs-*,cluster-a:failing | STATS total=sum(const) | LIMIT 1",
            includeCCSMetadata.v1()
        );
        // wait until we know that the query against remote has started
        FailingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
        // Allow to proceed
        FailingPauseFieldPlugin.allowEmitting.countDown();

        // wait until local queries have finished
        try {
            assertBusy(() -> assertThrows(Exception.class, () -> getAsyncResponse(client(), asyncExecutionId)));
            // Ensure stop query fails too when get fails
            assertThrows(
                ElasticsearchException.class,
                () -> client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(asyncExecutionId)).actionGet()
            );
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    private String randomAsyncId() {
        return AsyncExecutionId.encode(randomAlphaOfLength(10), new TaskId(randomAlphaOfLength(10), randomLong()));
    }

    public void testBadAsyncId() throws Exception {
        setupClusters(3);
        final AtomicReference<String> asyncId = new AtomicReference<>();
        try (
            EsqlQueryResponse resp = runAsyncQuery(
                client(),
                "FROM logs-*,*:logs-* | STATS total=sum(const) | LIMIT 1",
                randomBoolean(),
                TimeValue.timeValueMillis(0)
            )
        ) {
            assertTrue(resp.isRunning());
            asyncId.set(resp.asyncExecutionId().get());
        }
        assertBusy(() -> {
            try (EsqlQueryResponse resp = getAsyncResponse(client(), asyncId.get())) {
                assertFalse(resp.isRunning());
            }
        });

        String randomAsyncIdasyncId = randomAsyncId();
        var stopRequest = new AsyncStopRequest(randomAsyncIdasyncId);
        var stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
        assertThrows(ResourceNotFoundException.class, () -> stopAction.actionGet(1000, TimeUnit.SECONDS));
    }
}
