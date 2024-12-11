/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class); // allows the async_search DELETE action
        plugins.add(InternalExchangePlugin.class);
        plugins.add(PauseFieldPlugin.class);
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
        PauseFieldPlugin.allowEmitting = new CountDownLatch(1);
        PauseFieldPlugin.startEmitting = new CountDownLatch(1);
    }

    public static class PauseFieldPlugin extends Plugin implements ScriptPlugin {
        public static CountDownLatch startEmitting = new CountDownLatch(1);
        public static CountDownLatch allowEmitting = new CountDownLatch(1);

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override

                public String getType() {
                    return "pause";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context == LongFieldScript.CONTEXT) {
                        return (FactoryType) new LongFieldScript.Factory() {
                            @Override
                            public LongFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        startEmitting.countDown();
                                        try {
                                            assertTrue(allowEmitting.await(30, TimeUnit.SECONDS));
                                        } catch (InterruptedException e) {
                                            throw new AssertionError(e);
                                        }
                                        emit(1);
                                    }
                                };
                            }
                        };
                    }
                    throw new IllegalStateException("unsupported type " + context);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }

    /**
     * Includes testing for CCS metadata in the GET /_query/async/:id response while the search is still running
     */
    public void testSuccessfulPathways() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.blocking_index.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        AtomicReference<String> asyncExecutionId = new AtomicReference<>();

        String q = "FROM logs-*,cluster-a:logs-*,remote-b:blocking | STATS total=sum(const) | LIMIT 10";
        try (EsqlQueryResponse resp = runAsyncQuery(q, requestIncludeMeta, null, TimeValue.timeValueMillis(100))) {
            assertTrue(resp.isRunning());
            assertNotNull("async execution id is null", resp.asyncExecutionId());
            asyncExecutionId.set(resp.asyncExecutionId().get());
            // executionInfo may or may not be set on the initial response when there is a relatively low wait_for_completion_timeout
            // so we do not check for it here
        }

        // wait until we know that the query against 'remote-b:blocking' has started
        PauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

        // wait until the query of 'cluster-a:logs-*' has finished (it is not blocked since we are not searching the 'blocking' index on it)
        assertBusy(() -> {
            try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId.get())) {
                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster clusterA = executionInfo.getCluster("cluster-a");
                assertThat(clusterA.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
            }
        });

        /* at this point:
         *  the query against cluster-a should be finished
         *  the query against remote-b should be running (blocked on the PauseFieldPlugin.allowEmitting CountDown)
         *  the query against the local cluster should be running because it has a STATS clause that needs to wait on remote-b
         */
        try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId.get())) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            assertThat(asyncResponse.isRunning(), is(true));
            assertThat(
                executionInfo.clusterAliases(),
                equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(executionInfo.getClusterStateCount(EsqlExecutionInfo.Cluster.Status.RUNNING), equalTo(2));
            assertThat(executionInfo.getClusterStateCount(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL), equalTo(1));

            EsqlExecutionInfo.Cluster clusterA = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(clusterA.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(clusterA.getTotalShards(), greaterThanOrEqualTo(1));
            assertThat(clusterA.getSuccessfulShards(), equalTo(clusterA.getTotalShards()));
            assertThat(clusterA.getSkippedShards(), equalTo(0));
            assertThat(clusterA.getFailedShards(), equalTo(0));
            assertThat(clusterA.getFailures().size(), equalTo(0));
            assertThat(clusterA.getTook().millis(), greaterThanOrEqualTo(0L));

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
        PauseFieldPlugin.allowEmitting.countDown();

        // wait until both remoteB and local queries have finished
        assertBusy(() -> {
            try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId.get())) {
                EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster remoteB = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remoteB.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
                EsqlExecutionInfo.Cluster local = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                assertThat(local.getStatus(), not(equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING)));
                assertThat(asyncResponse.isRunning(), is(false));
            }
        });

        try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId.get())) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(1L));

            EsqlExecutionInfo.Cluster clusterA = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(clusterA.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(clusterA.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(clusterA.getTotalShards(), equalTo(remote1NumShards));
            assertThat(clusterA.getSuccessfulShards(), equalTo(remote1NumShards));
            assertThat(clusterA.getSkippedShards(), equalTo(0));
            assertThat(clusterA.getFailedShards(), equalTo(0));
            assertThat(clusterA.getFailures().size(), equalTo(0));

            EsqlExecutionInfo.Cluster remoteB = executionInfo.getCluster(REMOTE_CLUSTER_2);
            assertThat(remoteB.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteB.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteB.getTotalShards(), equalTo(remote2NumShards));
            assertThat(remoteB.getSuccessfulShards(), equalTo(remote2NumShards));
            assertThat(remoteB.getSkippedShards(), equalTo(0));
            assertThat(remoteB.getFailedShards(), equalTo(0));
            assertThat(remoteB.getFailures().size(), equalTo(0));

            EsqlExecutionInfo.Cluster local = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(local.getTotalShards(), equalTo(localNumShards));
            assertThat(local.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(local.getSkippedShards(), equalTo(0));
            assertThat(local.getFailedShards(), equalTo(0));
            assertThat(local.getFailures().size(), equalTo(0));
        } finally {
            AcknowledgedResponse acknowledgedResponse = deleteAsyncId(asyncExecutionId.get());
            assertThat(acknowledgedResponse.isAcknowledged(), is(true));
        }
    }

    public void testAsyncQueriesWithLimit0() throws IOException {
        setupClusters(3);
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        final TimeValue waitForCompletion = TimeValue.timeValueNanos(randomFrom(1L, Long.MAX_VALUE));
        String asyncExecutionId = null;
        try (EsqlQueryResponse resp = runAsyncQuery("FROM logs*,*:logs* | LIMIT 0", requestIncludeMeta, null, waitForCompletion)) {
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

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remoteCluster.getTotalShards(), equalTo(0));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                assertClusterMetadataInResponse(resp, responseExpectMeta, 3);
            }
        } finally {
            if (asyncExecutionId != null) {
                AcknowledgedResponse acknowledgedResponse = deleteAsyncId(asyncExecutionId);
                assertThat(acknowledgedResponse.isAcknowledged(), is(true));
            }
        }
    }

    protected EsqlQueryResponse runAsyncQuery(String query, Boolean ccsMetadata, QueryBuilder filter, TimeValue waitCompletionTime) {
        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadata != null) {
            request.includeCCSMetadata(ccsMetadata);
        }
        request.waitForCompletionTimeout(waitCompletionTime);
        request.keepOnCompletion(false);
        if (filter != null) {
            request.filter(filter);
        }
        return runAsyncQuery(request);
    }

    protected EsqlQueryResponse runAsyncQuery(EsqlQueryRequest request) {
        try {
            return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for query response", e);
        }
    }

    AcknowledgedResponse deleteAsyncId(String id) {
        try {
            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            return client().execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for DELETE response", e);
        }
    }

    EsqlQueryResponse getAsyncResponse(String id) {
        try {
            var getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(timeValueMillis(1));
            return client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout waiting for GET async result", e);
        }
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

    /**
     * v1: value to send to runQuery (can be null; null means use default value)
     * v2: whether to expect CCS Metadata in the response (cannot be null)
     * @return
     */
    public static Tuple<Boolean, Boolean> randomIncludeCCSMetadata() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> new Tuple<>(Boolean.TRUE, Boolean.TRUE);
            case 2 -> new Tuple<>(Boolean.FALSE, Boolean.FALSE);
            case 3 -> new Tuple<>(null, Boolean.FALSE);
            default -> throw new AssertionError("should not get here");
        };
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
            populateRemoteIndicesWithRuntimeMapping(REMOTE_CLUSTER_2);
            clusterInfo.put("remote2.index", REMOTE_INDEX);
            clusterInfo.put("remote2.num_shards", numShardsRemote2);
            clusterInfo.put("remote2.blocking_index", INDEX_WITH_RUNTIME_MAPPING);
            clusterInfo.put("remote2.blocking_index.num_shards", 1);
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

    void populateRemoteIndicesWithRuntimeMapping(String clusterAlias) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        client(clusterAlias).admin().indices().prepareCreate(INDEX_WITH_RUNTIME_MAPPING).setMapping(mapping).get();
        BulkRequestBuilder bulk = client(clusterAlias).prepareBulk(INDEX_WITH_RUNTIME_MAPPING)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10; i++) {
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
}
