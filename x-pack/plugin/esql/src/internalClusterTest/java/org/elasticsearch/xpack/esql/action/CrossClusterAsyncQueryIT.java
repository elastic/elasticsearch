/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.runAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQueryWithPragmas;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.waitForCluster;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CrossClusterAsyncQueryIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "remote-b";
    private static String LOCAL_INDEX = "logs-1";
    private static String REMOTE_INDEX = "logs-2";
    private static final String INDEX_WITH_RUNTIME_MAPPING = "blocking";
    private static final String INDEX_WITH_FAIL_MAPPING = "failing";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class); // allows the async_search DELETE action
        plugins.add(InternalExchangePlugin.class);
        plugins.add(SimplePauseFieldPlugin.class);
        plugins.add(FailingPauseFieldPlugin.class);
        plugins.add(CountingPauseFieldPlugin.class);
        return plugins;
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    @Before
    public void resetPlugin() {
        SimplePauseFieldPlugin.resetPlugin();
        FailingPauseFieldPlugin.resetPlugin();
        CountingPauseFieldPlugin.resetPlugin();
    }

    /**
     * Includes testing for CCS metadata in the GET /_query/async/:id response while the search is still running
     */
    public void testSuccessfulPathways() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        populateRuntimeIndex(REMOTE_CLUSTER_2, "pause", INDEX_WITH_RUNTIME_MAPPING);

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
                assertThat(resp.columns().contains(new ColumnInfoImpl("const", "long")), is(true));
                assertThat(resp.columns().contains(new ColumnInfoImpl("id", "keyword")), is(true));
                assertThat(resp.columns().contains(new ColumnInfoImpl("tag", "keyword")), is(true));
                assertThat(resp.columns().contains(new ColumnInfoImpl("v", "long")), is(true));
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

    public void testStopQuery() throws Exception {
        assumeTrue("Pragme does not work in release builds", Build.current().isSnapshot());
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        // Create large index so we could be sure we're stopping before the end
        populateRuntimeIndex(REMOTE_CLUSTER_2, "pause_count", INDEX_WITH_RUNTIME_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQueryWithPragmas(
            client(),
            "FROM logs-*,cluster-a:logs-*,remote-b:blocking | STATS total=sum(coalesce(const,v)) | LIMIT 1",
            includeCCSMetadata.v1(),
            Map.of("page_size", 1, "data_partitioning", "shard", "task_concurrency", 1)
        );

        // wait until we know that the query against 'remote-b:blocking' has started
        CountingPauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

        // wait until the query of 'cluster-a:logs-*' has finished (it is not blocked since we are not searching the 'blocking' index on it)
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
            List<TaskInfo> reduceTasks = tasks.stream().filter(t -> t.description().contains("_LuceneSourceOperator") == false).toList();
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
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    public void testStopQueryLocal() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        populateRuntimeIndex(LOCAL_CLUSTER, "pause", INDEX_WITH_RUNTIME_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM blocking,*:logs-* | STATS total=sum(coalesce(const,v)) | LIMIT 1",
            includeCCSMetadata.v1()
        );

        // wait until we know that the query against 'remote-b:blocking' has started
        SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

        // wait until the remotes are done
        waitForCluster(client(), REMOTE_CLUSTER_1, asyncExecutionId);
        waitForCluster(client(), REMOTE_CLUSTER_2, asyncExecutionId);

        /* at this point:
         *  the query against remotes should be finished
         *  the query against the local cluster should be running because it's blocked
         */

        // run the stop query
        AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId);
        ActionFuture<EsqlQueryResponse> stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
        // ensure stop operation is running
        assertBusy(() -> {
            try (EsqlQueryResponse asyncResponse = getAsyncResponse(client(), asyncExecutionId)) {
                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isPartial(), is(true));
            }
        });
        // allow local query to proceed
        SimplePauseFieldPlugin.allowEmitting.countDown();

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
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    public void testStopQueryLocalNoRemotes() throws Exception {
        setupClusters(3);
        populateRuntimeIndex(LOCAL_CLUSTER, "pause", INDEX_WITH_RUNTIME_MAPPING);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final String asyncExecutionId = startAsyncQuery(
            client(),
            "FROM blocking | STATS total=count(const) | LIMIT 1",
            includeCCSMetadata.v1()
        );

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
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
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

    private void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster cluster, int numShards) {
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(cluster.getTotalShards(), equalTo(numShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(numShards));
        assertThat(cluster.getSkippedShards(), equalTo(0));
        assertThat(cluster.getFailedShards(), equalTo(0));
        assertThat(cluster.getFailures().size(), equalTo(0));
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean responseExpectMeta, int numClusters) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");
            if (responseExpectMeta) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertThat((int) inner.get("total"), equalTo(numClusters));
                assertTrue(inner.containsKey("details"));
            } else {
                assertNull(clusters);
            }
        } catch (IOException e) {
            fail("Could not convert ESQLQueryResponse to Map: " + e);
        }
    }

    Map<String, Object> setupClusters(int numClusters) throws IOException {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(LOCAL_INDEX, numShardsLocal);

        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote1.num_shards", numShardsRemote);
        clusterInfo.put("remote1.index", REMOTE_INDEX);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            populateRemoteIndices(REMOTE_CLUSTER_2, REMOTE_INDEX, numShardsRemote2);
            clusterInfo.put("remote2.index", REMOTE_INDEX);
            clusterInfo.put("remote2.num_shards", numShardsRemote2);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);

        return clusterInfo;
    }

    void populateLocalIndices(String indexName, int numShards) {
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long", "const", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            localClient.prepareIndex(indexName).setSource("id", "local-" + i, "tag", "local", "v", i).get();
        }
        localClient.admin().indices().prepareRefresh(indexName).get();
    }

    void populateRuntimeIndex(String clusterAlias, String langName, String indexName) throws IOException {
        populateRuntimeIndex(clusterAlias, langName, indexName, 10);
    }

    void populateRuntimeIndex(String clusterAlias, String langName, String indexName, int count) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", langName).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        client(clusterAlias).admin().indices().prepareCreate(indexName).setMapping(mapping).get();
        BulkRequestBuilder bulk = client(clusterAlias).prepareBulk(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < count; i++) {
            bulk.add(new IndexRequest().source("foo", i));
        }
        bulk.get();
    }

    void populateRemoteIndices(String clusterAlias, String indexName, int numShards) throws IOException {
        Client remoteClient = client(clusterAlias);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            remoteClient.prepareIndex(indexName).setSource("id", "remote-" + i, "tag", "remote", "v", i * i).get();
        }
        remoteClient.admin().indices().prepareRefresh(indexName).get();
    }

    public static class CountingPauseFieldPlugin extends SimplePauseFieldPlugin {
        public static AtomicLong count = new AtomicLong(0);

        protected String scriptTypeName() {
            return "pause_count";
        }

        public static void resetPlugin() {
            count.set(0);
        }

        @Override
        public boolean onWait() throws InterruptedException {
            count.incrementAndGet();
            return allowEmitting.await(30, TimeUnit.SECONDS);
        }
    }

    private static List<TaskInfo> getDriverTasks(Client client) {
        return client.admin().cluster().prepareListTasks().setActions(DriverTaskRunner.ACTION_NAME).setDetailed(true).get().getTasks();
    }
}
