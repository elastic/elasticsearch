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

package org.elasticsearch.search;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;

public class CrossClusterSearchUnavailableClusterIT extends ESRestTestCase {

    private static RestHighLevelClient restHighLevelClient;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static MockTransportService startTransport(
            final String id,
            final List<DiscoveryNode> knownNodes,
            final Version version,
            final ThreadPool threadPool) {
        boolean success = false;
        final Settings s = Settings.builder().put("node.name", id).build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, threadPool, null);
        try {
            newService.registerRequestHandler(ClusterSearchShardsAction.NAME, ThreadPool.Names.SAME, ClusterSearchShardsRequest::new,
                (request, channel, task) -> {
                        channel.sendResponse(new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0],
                                knownNodes.toArray(new DiscoveryNode[0]), Collections.emptyMap()));
                    });
            newService.registerRequestHandler(SearchAction.NAME, ThreadPool.Names.SAME, SearchRequest::new,
                (request, channel, task) -> {
                    InternalSearchResponse response = new InternalSearchResponse(new SearchHits(new SearchHit[0],
                        new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN), InternalAggregations.EMPTY, null, null, false, null, 1);
                    SearchResponse searchResponse = new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY);
                    channel.sendResponse(searchResponse);
                });
            newService.registerRequestHandler(ClusterStateAction.NAME, ThreadPool.Names.SAME, ClusterStateRequest::new,
                (request, channel, task) -> {
                        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                        for (DiscoveryNode node : knownNodes) {
                            builder.add(node);
                        }
                        ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                        channel.sendResponse(new ClusterStateResponse(clusterName, build, false));
                    });
            newService.start();
            newService.acceptIncomingRequests();
            success = true;
            return newService;
        } finally {
            if (success == false) {
                newService.close();
            }
        }
    }

    public void testSearchSkipUnavailable() throws IOException {
        try (MockTransportService remoteTransport = startTransport("node0", new CopyOnWriteArrayList<>(), Version.CURRENT, threadPool)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            updateRemoteClusterSettings(Collections.singletonMap("seeds", remoteNode.getAddress().toString()));

            for (int i = 0; i < 10; i++) {
                restHighLevelClient.index(
                        new IndexRequest("index").id(String.valueOf(i)).source("field", "value"), RequestOptions.DEFAULT);
            }
            Response refreshResponse = client().performRequest(new Request("POST", "/index/_refresh"));
            assertEquals(200, refreshResponse.getStatusLine().getStatusCode());

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index"), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, response.getClusters());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(2, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT);
                assertEquals(1, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(0, response.getHits().getTotalHits().value);
            }

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index").scroll("1m"),
                        RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(2, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
                String scrollId = response.getScrollId();
                SearchResponse scrollResponse = restHighLevelClient.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, scrollResponse.getClusters());
                assertEquals(10, scrollResponse.getHits().getTotalHits().value);
                assertEquals(0, scrollResponse.getHits().getHits().length);
            }

            remoteTransport.close();

            updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", true));

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT);
                assertEquals(1, response.getClusters().getTotal());
                assertEquals(0, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(0, response.getHits().getTotalHits().value);
            }

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index").scroll("1m"),
                        RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
                String scrollId = response.getScrollId();
                SearchResponse scrollResponse = restHighLevelClient.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, scrollResponse.getClusters());
                assertEquals(10, scrollResponse.getHits().getTotalHits().value);
                assertEquals(0, scrollResponse.getHits().getHits().length);
            }

            updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", false));
            assertSearchConnectFailure();

            Map<String, Object> map = new HashMap<>();
            map.put("seeds", null);
            map.put("skip_unavailable", null);
            updateRemoteClusterSettings(map);
        }
    }

    public void testSkipUnavailableDependsOnSeeds() throws IOException {
        try (MockTransportService remoteTransport = startTransport("node0", new CopyOnWriteArrayList<>(), Version.CURRENT, threadPool)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            {
                //check that skip_unavailable alone cannot be set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(
                    Collections.singletonMap("skip_unavailable", randomBoolean())));
                ResponseException responseException = expectThrows(ResponseException.class,
                        () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(responseException.getMessage(),
                        containsString("missing required setting [cluster.remote.remote1.seeds] " +
                                "for setting [cluster.remote.remote1.skip_unavailable]"));
            }

            Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put("seeds", remoteNode.getAddress().toString());
            settingsMap.put("skip_unavailable", randomBoolean());
            updateRemoteClusterSettings(settingsMap);

            {
                //check that seeds cannot be reset alone if skip_unavailable is set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(Collections.singletonMap("seeds", null)));
                ResponseException responseException = expectThrows(ResponseException.class,
                        () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(responseException.getMessage(), containsString("missing required setting [cluster.remote.remote1.seeds] " +
                        "for setting [cluster.remote.remote1.skip_unavailable]"));
            }

            if (randomBoolean()) {
                updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", null));
                updateRemoteClusterSettings(Collections.singletonMap("seeds", null));
            } else {
                Map<String, Object> nullMap = new HashMap<>();
                nullMap.put("seeds", null);
                nullMap.put("skip_unavailable", null);
                updateRemoteClusterSettings(nullMap);
            }
        }
    }

    private static void assertSearchConnectFailure() {
        {
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT));
            ElasticsearchException rootCause = (ElasticsearchException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
        {
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT));
            ElasticsearchException rootCause = (ElasticsearchException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
        {
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> restHighLevelClient.search(new SearchRequest("remote1:index").scroll("1m"), RequestOptions.DEFAULT));
            ElasticsearchException rootCause = (ElasticsearchException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
    }



    private static void updateRemoteClusterSettings(Map<String, Object> settings) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setEntity(buildUpdateSettingsRequestBody(settings));
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static HttpEntity buildUpdateSettingsRequestBody(Map<String, Object> settings) throws IOException {
        String requestBody;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("persistent");
                {
                    builder.startObject("cluster.remote.remote1");
                    {
                        for (Map.Entry<String, Object> entry : settings.entrySet()) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            requestBody = Strings.toString(builder);
        }
        return new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }
}
