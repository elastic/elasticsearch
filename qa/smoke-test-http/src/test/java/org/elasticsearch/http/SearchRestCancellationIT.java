/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class SearchRestCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ScriptedBlockPlugin.class);
    }

    public void testAutomaticCancellationDuringQueryPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        verifyCancellationDuringQueryPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringQueryPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new NByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringQueryPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringQueryPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        PlainActionFuture<Response> future = PlainActionFuture.newFuture();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, wrapAsRestResponseListener(future));

        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        expectThrows(CancellationException.class, future::actionGet);
    }

    public void testAutomaticCancellationDuringFetchPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().scriptField("test_field",
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        verifyCancellationDuringFetchPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringFetchPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new NByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringFetchPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringFetchPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        PlainActionFuture<Response> future = PlainActionFuture.newFuture();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, wrapAsRestResponseListener(future));

        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        expectThrows(CancellationException.class, future::actionGet);
    }

    private static Map<String, String> readNodesInfo() {
        Map<String, String> nodeIdToName = new HashMap<>();
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            nodeIdToName.put(node.getNode().getId(), node.getNode().getName());
        }
        return nodeIdToName;
    }

    private static void ensureSearchTaskIsCancelled(String transportAction, Function<String, String> nodeIdToName) throws Exception {
        SetOnce<TaskInfo> searchTask = new SetOnce<>();
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().get();
        for (TaskInfo task : listTasksResponse.getTasks()) {
            if (task.getAction().equals(transportAction)) {
                searchTask.set(task);
            }
        }
        assertNotNull(searchTask.get());
        TaskId taskId = searchTask.get().getTaskId();
        String nodeName = nodeIdToName.apply(taskId.getNodeId());
        assertBusy(() -> {
            TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
            Task task = taskManager.getTask(taskId.getId());
            assertThat(task, instanceOf(CancellableTask.class));
            assertTrue(((CancellableTask)task).isCancelled());
        });
    }

    private static void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test").setId(Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private static List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        }, 10, TimeUnit.SECONDS);
    }

    private static void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        void reset() {
            hits.set(0);
        }

        void disableBlock() {
            shouldBlock.set(false);
        }

        void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafStoredFieldsLookup fieldsLookup = (LeafStoredFieldsLookup) params.get("_fields");
                LogManager.getLogger(SearchRestCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    waitUntil(() -> shouldBlock.get() == false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    private static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }
}
