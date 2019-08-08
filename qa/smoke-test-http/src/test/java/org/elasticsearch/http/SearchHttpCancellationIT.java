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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.TargetAuthenticationStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.WriteRequest;
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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.http.SearchHttpCancellationIT.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class SearchHttpCancellationIT extends HttpSmokeTestCase {

    private static CloseableHttpAsyncClient client;
    private static HttpHost httpHost;
    private static Map<String, String> nodeIdToName = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(ScriptedBlockPlugin.class);
        plugins.addAll(super.nodePlugins());
        return plugins;
    }

/*
    @Before
    public void init() throws Exception {
        if (client == null) {
            try {
                client = AccessController.doPrivileged((PrivilegedExceptionAction<CloseableHttpAsyncClient>)
                    () -> client = HttpAsyncClientBuilder.create().build());
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    client.start();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw (Exception) e.getCause();
            }
            readNodesInfo(nodeIdToName);
        }
    }
*/

/*
    @AfterClass
    public static void closeClient() throws IOException {
        IOUtils.close(client);
        client = null;
    }
*/

    public void testAutomaticCancellationDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));

        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());

        HttpHost httpHost = null;
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            if (node.getHttp() != null) {
                TransportAddress publishAddress = node.getHttp().address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                httpHost = new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http");
                break;
            }
        }


        try (CloseableHttpAsyncClient httpClient = AccessController.doPrivileged(
            (PrivilegedAction<CloseableHttpAsyncClient>) this::createHttpClient)) {
            httpClient.start();

            HttpPost httpPost = new HttpPost("/test/_search");
            httpPost.setEntity(new NStringEntity(Strings.toString(searchSource), ContentType.APPLICATION_JSON));
            HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(httpHost, httpPost);
            BasicAsyncResponseConsumer responseConsumer = new BasicAsyncResponseConsumer();
            HttpClientContext context = HttpClientContext.create();
            AtomicReference<Exception> error = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            httpClient.execute(requestProducer, responseConsumer, context, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse result) {
                    latch.countDown();
                }

                @Override
                public void failed(Exception ex) {
                    error.set(ex);
                    latch.countDown();
                }

                @Override
                public void cancelled() {

                }
            });
            latch.await();
            throw error.get();
        }

/*
        CountDownLatch latch = new CountDownLatch(1);
        Request post = new Request("POST", "/test/_search");
        post.setJsonEntity(Strings.toString(searchSource));
        getRestClient().performRequestAsync(post, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                System.out.println("response onSuccess");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                System.out.println("response onFailure");
                exception.printStackTrace();
                latch.countDown();
            }
        });

        awaitForBlock(plugins);

        disableBlocks(plugins);

        latch.await();
*/


        //sendRequest(httpHost, new HttpGet("/"));

        /*HttpPost httpPost = new HttpPost("/test/_search");

        httpPost.setEntity(new NStringEntity(Strings.toString(searchSource), ContentType.APPLICATION_JSON));
        HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(httpHost, httpPost);
        HttpAsyncResponseConsumer<HttpResponse> httpAsyncResponseConsumer =
            HttpAsyncResponseConsumerFactory.DEFAULT.createHttpAsyncResponseConsumer();
        HttpClientContext context = HttpClientContext.create();

        Future<HttpResponse> future = client.execute(requestProducer, httpAsyncResponseConsumer, context, null);
        future.get();//TODO remove
        awaitForBlock(plugins);

        httpPost.abort();
        ensureSearchTaskIsCancelled(nodeIdToName::get);

        disableBlocks(plugins);
        expectThrows(CancellationException.class, future::get);*/
    }

    private CloseableHttpAsyncClient createHttpClient() {
        //default timeouts are all infinite
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
            .setConnectTimeout(1000)
            .setSocketTimeout(30_000);

        HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create().setDefaultRequestConfig(requestConfigBuilder.build())
            //default settings for connection pooling may be too constraining
            .setMaxConnPerRoute(10).setMaxConnTotal(30)
            .setTargetAuthenticationStrategy(TargetAuthenticationStrategy.INSTANCE);
        httpClientBuilder.setThreadFactory(Executors.privilegedThreadFactory());
        return AccessController.doPrivileged((PrivilegedAction<CloseableHttpAsyncClient>) httpClientBuilder::build);
    }

/*    public void testAutomaticCancellationDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        HttpPost httpPost = new HttpPost("/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().scriptField("test_field",
            new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap()));
        httpPost.setEntity(new NStringEntity(Strings.toString(searchSource), ContentType.APPLICATION_JSON));

        Future<HttpResponse> future = sendRequest(httpHost, httpPost);
        future.get();//TODO remove
        awaitForBlock(plugins);

        httpPost.abort();
        ensureSearchTaskIsCancelled(nodeIdToName::get);

        disableBlocks(plugins);
        expectThrows(CancellationException.class, future::get);
    }*/

    private static Future<HttpResponse> sendRequest(HttpHost httpHost, HttpRequestBase httpRequest) throws Exception {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Future<HttpResponse>>)
                () -> client.execute(httpHost, httpRequest, null));
        } catch (PrivilegedActionException e) {
            throw (Exception) e.getCause();
        }
    }

    private static void readNodesInfo(Map<String, String> nodeIdToName) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        List<HttpHost> hosts = new ArrayList<>();
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            if (node.getHttp() != null) {
                TransportAddress publishAddress = node.getHttp().address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                hosts.add(new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http"));
            }
            nodeIdToName.put(node.getNode().getId(), node.getNode().getName());
        }
        httpHost = randomFrom(hosts);
    }

    private static void ensureSearchTaskIsCancelled(Function<String, String> nodeIdToName) {
        SetOnce<TaskInfo> searchTask = new SetOnce<>();
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().get();
        for (TaskInfo task : listTasksResponse.getTasks()) {
            if (task.getAction().equals(SearchAction.NAME)) {
                searchTask.set(task);
            }
        }
        assertNotNull(searchTask.get());
        TaskId taskId = searchTask.get().getTaskId();
        String nodeName = nodeIdToName.apply(taskId.getNodeId());
        TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
        Task task = taskManager.getTask(taskId.getId());
        assertThat(task, instanceOf(CancellableTask.class));
        assertTrue(((CancellableTask)task).isCancelled());
    }

    private static void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test", "_doc", Integer.toString(i * 5 + j)).setSource("field", "value"));
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
        }, 20, TimeUnit.SECONDS);
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
