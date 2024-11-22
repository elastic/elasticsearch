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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
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

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterAsyncQueryIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "remote-b";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class); // allows the async_search DELETE action
        plugins.add(InternalExchangePlugin.class);
        plugins.add(PauseFieldPluginx.class);
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
        PauseFieldPluginx.allowEmittingx = new CountDownLatch(1);
        PauseFieldPluginx.startEmittingx = new CountDownLatch(1);
    }

    public static class PauseFieldPluginx extends Plugin implements ScriptPlugin {
        public static CountDownLatch startEmittingx = new CountDownLatch(1);
        public static CountDownLatch allowEmittingx = new CountDownLatch(1);

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override

                public String getType() {
                    System.err.println("PAUSE DEBUG 0: getType");
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
                                System.err.println("PAUSE DEBUG 1. FieldName: " + fieldName);
                                return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        System.err.println("PAUSE DEBUG 2");
                                        startEmittingx.countDown();
                                        System.err.println("PAUSE DEBUG 3: allowEmitting count: " + allowEmittingx.getCount());
                                        try {
                                            assertTrue(allowEmittingx.await(30, TimeUnit.SECONDS));
                                            System.err.println("PAUSE DEBUG 4");
                                        } catch (InterruptedException e) {
                                            System.err.println("PAUSE DEBUG 5");
                                            throw new AssertionError(e);
                                        }
                                        System.err.println("PAUSE DEBUG 6");
                                        emit(1);
                                    }
                                };
                            }
                        };
                    }
                    System.err.println("PAUSE DEBUG 7");
                    throw new IllegalStateException("unsupported type " + context);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }

//    public void xtestAsyncQueryWithPause() throws Exception {
//        Map<String, Object> testClusterInfo = setupClusters(2);
//        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest();
//        //request.query("FROM *:test | STATS total=sum(const) | LIMIT 1");
//        request.pragmas(randomPragmas());
//        PlainActionFuture<EsqlQueryResponse> requestFuture = new PlainActionFuture<>();
//
//        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
//        Boolean requestIncludeMeta = includeCCSMetadata.v1();
//        boolean responseExpectMeta = includeCCSMetadata.v2();
//        try (EsqlQueryResponse resp = runAsyncQuery("FROM cluster-a:test | STATS total=sum(const) | LIMIT 1", requestIncludeMeta, null)) {
//            System.err.println(Strings.toString(resp));
//            System.err.println(">> calling startEmitting.await on thread: " + Thread.currentThread().getName());
//            assertTrue(PauseFieldPluginx.startEmitting.await(2, TimeUnit.SECONDS));
//            String id = resp.asyncExecutionId().get();
//            System.err.println(id);
//            System.err.println("Sleeping on test thread: " + Thread.currentThread().getName());
//            Thread.sleep(2222);
//            EsqlQueryResponse getResponse1 = getAsyncResponse(id);
//            System.err.println(Strings.toString(getResponse1));
//            System.err.println(">> calling allowEmitting");
//            PauseFieldPluginx.allowEmitting.countDown();
//            Thread.sleep(2222);
//            EsqlQueryResponse getResponse2 = getAsyncResponse(id);
//            System.err.println(Strings.toString(getResponse2));
//        }

//        client().execute(EsqlQueryAction.INSTANCE, request, requestFuture);
//        List<TaskInfo> rootTasks = new ArrayList<>();
//        assertBusy(() -> {
//            List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
//            assertThat(tasks, hasSize(1));
//            rootTasks.addAll(tasks);
//        });
//        var cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTasks.get(0).taskId()).setReason("proxy timeout");
//        client().execute(TransportCancelTasksAction.TYPE, cancelRequest);
//        assertBusy(() -> {
//            List<TaskInfo> drivers = client(REMOTE_CLUSTER_1).admin()
//                .cluster()
//                .prepareListTasks()
//                .setActions(DriverTaskRunner.ACTION_NAME)
//                .get()
//                .getTasks();
//            assertThat(drivers.size(), greaterThanOrEqualTo(1));
//            for (TaskInfo driver : drivers) {
//                assertTrue(driver.cancellable());
//            }
//        });
//        PauseFieldPluginx.allowEmitting.countDown();
//        Exception error = expectThrows(Exception.class, requestFuture::actionGet);
//        assertThat(error.getMessage(), containsString("proxy timeout"));
//    }


    // MP TODO: this test is not robust, since it assumes the test will finish in 1 minute - need to build some ability to requery the
    //    getAsyncResponse if it is not finished
    public void testSuccessfulPathways() throws IOException {
        Map<String, Object> testClusterInfo = setupClusters(3);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();
        String q = "FROM logs-*,*:logs-* | STATS sum (v)";
        try (EsqlQueryResponse resp = runAsyncQuery(q, requestIncludeMeta, null, TimeValue.timeValueMinutes(1))) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(615L)));

            // MP TODO: the ExecutionInfo is not in the original response from async-search - need to fix that?
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster remoteCluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
            assertThat(remoteCluster2.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster2.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster2.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster2.getTotalShards(), equalTo(remote2NumShards));
            assertThat(remoteCluster2.getSuccessfulShards(), equalTo(remote2NumShards));
            assertThat(remoteCluster2.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster2.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

            // ensure that the _clusters metadata is present only if requested
            assertClusterMetadataInResponse(resp, responseExpectMeta);
        }

        // this test is also not robust, but does NOT trigger anything in the PausePlugin
        System.err.println("----------- SECOND QUERY against remote-b:test ----");
        q = "FROM logs-*,remote-b:test | STATS count(*)";
        String asyncExecutionId;
        try (EsqlQueryResponse resp = runAsyncQuery(q, requestIncludeMeta, null, TimeValue.timeValueNanos(1))) {
            System.err.println("Init resp: " + Strings.toString(resp));
            System.err.println("Init Resp execInfo: " + resp.getExecutionInfo());
            assertTrue(resp.isRunning());
            asyncExecutionId = resp.asyncExecutionId().get();
            System.err.println(asyncExecutionId);
            try {
                Thread.sleep(1431);
            } catch (InterruptedException e) {
            }
        }
        try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId)) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            System.err.println("GET Async execInfo: " + executionInfo);
            System.err.println("Async response: " + Strings.toString(asyncResponse));
        } finally {
            System.err.println("DELETING async search: " + asyncExecutionId);
            AcknowledgedResponse acknowledgedResponse = deleteAsyncId(asyncExecutionId);
            assertThat(acknowledgedResponse.isAcknowledged(), is(true));
        }

        q = "FROM logs-*,remote-b:test | STATS total=sum(const) | LIMIT 10";
        System.err.println("----- pause-based query: " + q);
        try (EsqlQueryResponse resp = runAsyncQuery(q, requestIncludeMeta, null, TimeValue.timeValueNanos(1))) {
            System.err.println("Init resp: " + Strings.toString(resp));
            System.err.println("Init Resp execInfo: " + resp.getExecutionInfo());
            assertTrue(resp.isRunning());
            asyncExecutionId = resp.asyncExecutionId().get();
            System.err.println(asyncExecutionId);
            System.err.println(">> BEFORE SLEEP PauseFieldPluginx.startEmittingx.getCount(): " + PauseFieldPluginx.startEmittingx.getCount());
            try {
                Thread.sleep(1431);
            } catch (InterruptedException e) {
            }
            System.err.println(">> AFTER SLEEP PauseFieldPluginx.startEmittingx.getCount(): " + PauseFieldPluginx.startEmittingx.getCount());
            PauseFieldPluginx.startEmittingx.await(4, TimeUnit.SECONDS);
            System.err.println("PauseFieldPluginx.startEmittingx.await finished <<< <<<");
            System.err.println(" -- now calling allowEmittingX.countDown; count before: " + PauseFieldPluginx.allowEmittingx.getCount());
            PauseFieldPluginx.allowEmittingx.countDown();
        } catch (InterruptedException e) {
            throw new RuntimeException("BOO BOO" + e);
        }
        try {
            Thread.sleep(2345);
        } catch (InterruptedException e) {
        }
        try (EsqlQueryResponse asyncResponse = getAsyncResponse(asyncExecutionId)) {
            EsqlExecutionInfo executionInfo = asyncResponse.getExecutionInfo();
            System.err.println("GET Async execInfo: " + executionInfo);
            System.err.println("Async response: " + Strings.toString(asyncResponse));
        } finally {
            System.err.println("DELETING async search: " + asyncExecutionId);
            AcknowledgedResponse acknowledgedResponse = deleteAsyncId(asyncExecutionId);
            assertThat(acknowledgedResponse.isAcknowledged(), is(true));
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

        // deliberately small timeout, to frequently trigger incomplete response
        request.waitForCompletionTimeout(waitCompletionTime);
        //request.waitForCompletionTimeout(TimeValue.timeValueNanos(1));
        request.keepOnCompletion(false);
        if (filter != null) {
            request.filter(filter);
        }

        var response = runAsyncQuery(request);
        System.err.println(">> response.asyncExecutionId().isPresent(): " + response.asyncExecutionId().isPresent());
        //assertTrue(response.asyncExecutionId().isPresent()); // MP TODO remove this
//        if (response.asyncExecutionId().isPresent()) {
//            List<ColumnInfo> initialColumns = null;
//            List<Page> initialPages = null;
//            String id = response.asyncExecutionId().get();
//            if (response.isRunning() == false) {
//                assertThat(request.keepOnCompletion(), is(true));
//                initialColumns = List.copyOf(response.columns());
//                initialPages = deepCopyOf(response.pages(), TestBlockFactory.getNonBreakingInstance());
//            } else {
//                assertThat(response.columns(), is(empty())); // no partial results
//                assertThat(response.pages(), is(empty()));
//            }
//            response.close();
//            var getResponse = getAsyncResponse(id);
//
//            // assert initial contents, if any, are the same as async get contents
//            if (initialColumns != null) {
//                assertEquals(initialColumns, getResponse.columns());
//                assertEquals(initialPages, getResponse.pages());
//            }
//
//            //assertDeletable(id);
//            return getResponse;
//        } else {
        return response;
    }

//    void assertDeletable(String id) {
//        var resp = deleteAsyncId(id);
//        assertTrue(resp.isAcknowledged());
//        // the stored response should no longer be retrievable
//        var e = expectThrows(ResourceNotFoundException.class, () -> getAsyncResponse(id));
//        assertThat(e.getMessage(), IsEqual.equalTo(id));
//    }

    AcknowledgedResponse deleteAsyncId(String id) {
        try {
            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            return client().execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public static List<Page> deepCopyOf(List<Page> pages, BlockFactory blockFactory) {
        return pages.stream().map(page -> deepCopyOf(page, blockFactory)).toList();
    }

    public static Page deepCopyOf(Page page, BlockFactory blockFactory) {
        Block[] blockCopies = new Block[page.getBlockCount()];
        for (int i = 0; i < blockCopies.length; i++) {
            blockCopies[i] = BlockUtils.deepCopyOf(page.getBlock(i), blockFactory);
        }
        return new Page(blockCopies);
    }

    EsqlQueryResponse getAsyncResponse(String id) {
        try {
            var getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(timeValueMillis(1));
            return client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    protected EsqlQueryResponse runAsyncQuery(EsqlQueryRequest request) {
        try {
            return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }


    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse) {  // MP TODO remove
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadataInResponse != null) {
            request.includeCCSMetadata(ccsMetadataInResponse);
        }
        return runQuery(request);
    }

    protected EsqlQueryResponse runQuery(EsqlQueryRequest request) {  // MP TODO remove
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean responseExpectMeta) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");
            if (responseExpectMeta) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertTrue(inner.containsKey("details"));
            } else {
                assertNull(clusters);
            }
        } catch (IOException e) {
            fail("Could not convert ESQL response to Map: " + e);
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

    private static String LOCAL_INDEX = "logs-1";
    private static String IDX_ALIAS = "alias1";
    private static String FILTERED_IDX_ALIAS = "alias-filtered-1";
    private static String REMOTE_INDEX = "logs-2";
    private static final String INDEX_WITH_RUNTIME_MAPPING = "test";

    Map<String, Object> setupClusters(int numClusters) throws IOException {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(LOCAL_INDEX, numShardsLocal);

        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            populateRemoteIndices(REMOTE_CLUSTER_2, REMOTE_INDEX, numShardsRemote2);
            populateRemoteIndicesWithRuntimeMapping(REMOTE_CLUSTER_2);
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
        BulkRequestBuilder bulk = client(clusterAlias).prepareBulk(INDEX_WITH_RUNTIME_MAPPING).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
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

    private void setSkipUnavailable(String clusterAlias, boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + clusterAlias + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable() {
        Settings.Builder settingsBuilder = Settings.builder()
            .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".skip_unavailable")
            .putNull("cluster.remote." + REMOTE_CLUSTER_2 + ".skip_unavailable");
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(settingsBuilder.build())
            .get();
    }
}
