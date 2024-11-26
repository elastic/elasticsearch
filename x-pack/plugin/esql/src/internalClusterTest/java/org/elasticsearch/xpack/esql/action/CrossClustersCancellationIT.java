/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomPragmas;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class CrossClustersCancellationIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(PauseFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of("cluster-a", true, "cluster-b", false);
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueMillis(between(3000, 4000)),
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

    private void createRemoteIndex(int numDocs) throws Exception {
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
        client(REMOTE_CLUSTER).admin().indices().prepareCreate("test").setMapping(mapping).get();
        BulkRequestBuilder bulk = client(REMOTE_CLUSTER).prepareBulk("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            bulk.add(new IndexRequest().source("foo", i));
        }
        bulk.get();
    }

    private void createLocalIndex(int numDocs) throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        client(LOCAL_CLUSTER).admin().indices().prepareCreate("test").setMapping(mapping).get();
        BulkRequestBuilder bulk = client(LOCAL_CLUSTER).prepareBulk("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            bulk.add(new IndexRequest().source("const", i));
        }
        bulk.get();
    }

    public void testCancel() throws Exception {
        createRemoteIndex(between(10, 100));
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM *:test | STATS total=sum(const) | LIMIT 1");
        request.pragmas(randomPragmas());
        PlainActionFuture<EsqlQueryResponse> requestFuture = new PlainActionFuture<>();
        client().execute(EsqlQueryAction.INSTANCE, request, requestFuture);
        assertTrue(PauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        List<TaskInfo> rootTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
            assertThat(tasks, hasSize(1));
            rootTasks.addAll(tasks);
        });
        var cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTasks.get(0).taskId()).setReason("proxy timeout");
        client().execute(TransportCancelTasksAction.TYPE, cancelRequest);
        assertBusy(() -> {
            List<TaskInfo> drivers = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .get()
                .getTasks();
            assertThat(drivers.size(), greaterThanOrEqualTo(1));
            for (TaskInfo driver : drivers) {
                assertTrue(driver.cancellable());
            }
        });
        PauseFieldPlugin.allowEmitting.countDown();
        Exception error = expectThrows(Exception.class, requestFuture::actionGet);
        assertThat(error.getMessage(), containsString("proxy timeout"));
    }

    public void testSameRemoteClusters() throws Exception {
        TransportAddress address = cluster(REMOTE_CLUSTER).getInstance(TransportService.class).getLocalNode().getAddress();
        int moreClusters = between(1, 5);
        for (int i = 0; i < moreClusters; i++) {
            String clusterAlias = REMOTE_CLUSTER + "-" + i;
            configureRemoteClusterWithSeedAddresses(clusterAlias, List.of(address));
        }
        int numDocs = between(10, 100);
        createRemoteIndex(numDocs);
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM *:test | STATS total=sum(const) | LIMIT 1");
        request.pragmas(randomPragmas());
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);
        try {
            try {
                assertBusy(() -> {
                    List<TaskInfo> tasks = client(REMOTE_CLUSTER).admin()
                        .cluster()
                        .prepareListTasks()
                        .setActions(ComputeService.CLUSTER_ACTION_NAME)
                        .get()
                        .getTasks();
                    assertThat(tasks, hasSize(moreClusters + 1));
                });
            } finally {
                PauseFieldPlugin.allowEmitting.countDown();
            }
            try (EsqlQueryResponse resp = future.actionGet(30, TimeUnit.SECONDS)) {
                // TODO: This produces incorrect results because data on the remote cluster is processed multiple times.
                long expectedCount = numDocs * (moreClusters + 1L);
                assertThat(getValuesList(resp), equalTo(List.of(List.of(expectedCount))));
            }
        } finally {
            for (int i = 0; i < moreClusters; i++) {
                String clusterAlias = REMOTE_CLUSTER + "-" + i;
                removeRemoteCluster(clusterAlias);
            }
        }
    }

    // Check that cancelling remote task with skip_unavailable=true produces partial
    public void testCancelSkipUnavailable() throws Exception {
        createRemoteIndex(between(10, 100));
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM *:test | STATS total=sum(const) | LIMIT 1");
        request.pragmas(randomPragmas());
        request.includeCCSMetadata(true);
        PlainActionFuture<EsqlQueryResponse> requestFuture = new PlainActionFuture<>();
        client().execute(EsqlQueryAction.INSTANCE, request, requestFuture);
        assertTrue(PauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        List<TaskInfo> rootTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(ComputeService.CLUSTER_ACTION_NAME)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(1));
            rootTasks.addAll(tasks);
        });
        var cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTasks.get(0).taskId()).setReason("remote failed");
        client(REMOTE_CLUSTER).execute(TransportCancelTasksAction.TYPE, cancelRequest);
        assertBusy(() -> {
            List<TaskInfo> drivers = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .get()
                .getTasks();
            assertThat(drivers.size(), greaterThanOrEqualTo(1));
            for (TaskInfo driver : drivers) {
                assertTrue(driver.cancellable());
            }
        });
        PauseFieldPlugin.allowEmitting.countDown();
        var resp = requestFuture.actionGet();
        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

        assertNotNull(executionInfo);
        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(REMOTE_CLUSTER);

        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(cluster.getFailures().size(), equalTo(1));
        assertThat(cluster.getFailures().get(0).getCause(), instanceOf(TaskCancelledException.class));
    }

    // Check that closing remote node with skip_unavailable=true produces partial
    public void testCloseSkipUnavailable() throws Exception {
        assumeTrue("Only snapshot builds have delay()", Build.current().isSnapshot());
        createRemoteIndex(between(1000, 5000));
        createLocalIndex(100);
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("""
                FROM test*,cluster-a:test* METADATA _index
                | EVAL cluster=MV_FIRST(SPLIT(_index, ":"))
                | WHERE CASE(cluster == "cluster-a", delay(1ms), true)
                | STATS total = sum(const) | LIMIT 1
            """);
        request.pragmas(randomPragmas());
        var requestFuture = client().execute(EsqlQueryAction.INSTANCE, request);
        assertTrue(PauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        assertBusy(() -> {
            List<TaskInfo> drivers = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .get()
                .getTasks();
            assertThat(drivers.size(), greaterThanOrEqualTo(1));
            for (TaskInfo driver : drivers) {
                assertTrue(driver.cancellable());
            }
        });
        PauseFieldPlugin.allowEmitting.countDown();
        cluster(REMOTE_CLUSTER).close();
        try (var resp = requestFuture.actionGet()) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);

            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0).size(), equalTo(1));
            // We can't be sure of the exact value here as we don't know if any data from remote came in, but all local data should be there
            assertThat((long) values.get(0).get(0), greaterThanOrEqualTo(4950L));

            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(REMOTE_CLUSTER);
            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);

            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getSuccessfulShards(), equalTo(1));

            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(cluster.getSuccessfulShards(), equalTo(0));
            assertThat(cluster.getFailures().size(), equalTo(1));
        }
    }
}
