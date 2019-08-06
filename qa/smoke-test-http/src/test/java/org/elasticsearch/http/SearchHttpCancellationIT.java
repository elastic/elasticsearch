/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.http;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.http.SearchHttpCancellationIT.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class SearchHttpCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(ScriptedBlockPlugin.class);
        plugins.addAll(super.nodePlugins());
        return plugins;
    }

    public void testAutomaticCancellationDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create();

        try (CloseableHttpAsyncClient httpClient = clientBuilder.build()) {
            httpClient.start();

            NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
            assertFalse(nodesInfoResponse.hasFailures());
            List<HttpHost> hosts = new ArrayList<>();
            for (NodeInfo node : nodesInfoResponse.getNodes()) {
                if (node.getHttp() != null) {
                    TransportAddress publishAddress = node.getHttp().address().publishAddress();
                    InetSocketAddress address = publishAddress.address();
                    hosts.add(new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http"));
                }
            }

            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(scriptQuery(
                new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));

            HttpPost httpPost = new HttpPost("/test/_search");
            httpPost.setEntity(new NStringEntity(Strings.toString(searchSource), ContentType.APPLICATION_JSON));

            HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(randomFrom(hosts), httpPost);

            HttpAsyncResponseConsumer<HttpResponse> httpAsyncResponseConsumer =
                HttpAsyncResponseConsumerFactory.DEFAULT.createHttpAsyncResponseConsumer();
            HttpClientContext context = HttpClientContext.create();

            Future<HttpResponse> future = httpClient.execute(requestProducer, httpAsyncResponseConsumer, context, null);

            awaitForBlock(plugins);

            SetOnce<TaskInfo> searchTask = new SetOnce<>();
            ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().get();
            for (TaskInfo task : listTasksResponse.getTasks()) {
                if (task.getAction().equals(SearchAction.NAME)) {
                    searchTask.set(task);
                }
            }
            assertNotNull(searchTask);

            httpPost.abort();



            TaskId taskId = searchTask.get().getTaskId();
            NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo(taskId.getNodeId()).get();
            String nodeName = nodesInfo.getNodes().get(0).getNode().getName();
            TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
            Task task = taskManager.getTask(taskId.getId());
            assertThat(task, instanceOf(CancellableTask.class));
            assertTrue(((CancellableTask)task).isCancelled());

            disableBlocks(plugins);
            expectThrows(CancellationException.class, future::get);
        }
    }

    private void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test", "type", Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
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
        });
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
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
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(SearchHttpCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    awaitBusy(() -> shouldBlock.get() == false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
