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

public class CrossClustersCancellationIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected List<String> remoteClusterAlias() {
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
        try {
            assertBusy(() -> {
                List<TaskInfo> drivers = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks();
                assertThat(drivers.size(), greaterThanOrEqualTo(1));
                for (TaskInfo driver : drivers) {
                    assertTrue(driver.cancelled());
                }
            });
        } finally {
            PauseFieldPlugin.allowEmitting.countDown();
        }
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

    public void testTasks() throws Exception {
        createRemoteIndex(between(10, 100));
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM *:test | STATS total=sum(const) | LIMIT 1");
        request.pragmas(randomPragmas());
        ActionFuture<EsqlQueryResponse> requestFuture = client().execute(EsqlQueryAction.INSTANCE, request);
        assertTrue(PauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
        try {
            assertBusy(() -> {
                List<TaskInfo> clusterTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(ComputeService.CLUSTER_ACTION_NAME)
                    .get()
                    .getTasks();
                assertThat(clusterTasks.size(), equalTo(1));
                List<TaskInfo> drivers = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .setTargetParentTaskId(clusterTasks.getFirst().taskId())
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks();
                assertThat(drivers.size(), equalTo(1));
                TaskInfo driver = drivers.getFirst();
                assertThat(driver.description(), equalTo("""
                    \\_ExchangeSourceOperator[]
                    \\_AggregationOperator[mode = INTERMEDIATE, aggs = sum of longs]
                    \\_ExchangeSinkOperator"""));
            });
        } finally {
            PauseFieldPlugin.allowEmitting.countDown();
        }
        requestFuture.actionGet(30, TimeUnit.SECONDS).close();
    }
}
