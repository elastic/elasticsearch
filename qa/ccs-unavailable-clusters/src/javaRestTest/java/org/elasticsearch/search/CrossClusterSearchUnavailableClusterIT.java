/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class CrossClusterSearchUnavailableClusterIT extends ESRestTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static MockTransportService startTransport(
        final String id,
        final List<DiscoveryNode> knownNodes,
        final VersionInformation version,
        final TransportVersion transportVersion,
        final ThreadPool threadPool
    ) {
        boolean success = false;
        final Settings s = Settings.builder().put("node.name", id).build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, transportVersion, threadPool, null);
        try {
            newService.registerRequestHandler(
                TransportSearchShardsAction.TYPE.name(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                SearchShardsRequest::new,
                (request, channel, task) -> {
                    channel.sendResponse(new SearchShardsResponse(List.of(), List.of(), Collections.emptyMap()));
                }
            );
            newService.registerRequestHandler(
                TransportSearchAction.TYPE.name(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                SearchRequest::new,
                (request, channel, task) -> channel.sendResponse(
                    new SearchResponse(
                        SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                        InternalAggregations.EMPTY,
                        null,
                        false,
                        null,
                        null,
                        1,
                        null,
                        1,
                        1,
                        0,
                        100,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY
                    )
                )
            );
            newService.registerRequestHandler(
                ClusterStateAction.NAME,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ClusterStateRequest::new,
                (request, channel, task) -> {
                    DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                    for (DiscoveryNode node : knownNodes) {
                        builder.add(node);
                    }
                    ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                    channel.sendResponse(new ClusterStateResponse(clusterName, build, false));
                }
            );
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
        try (
            MockTransportService remoteTransport = startTransport(
                "node0",
                new CopyOnWriteArrayList<>(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalNode();

            updateRemoteClusterSettings(Collections.singletonMap("seeds", remoteNode.getAddress().toString()));

            for (int i = 0; i < 10; i++) {
                Request request = new Request("POST", "/index/_doc");
                request.setJsonEntity("{ \"field\" : \"value\" }");
                Response response = client().performRequest(request);
                assertEquals(201, response.getStatusLine().getStatusCode());
            }
            Response refreshResponse = client().performRequest(new Request("POST", "/index/_refresh"));
            assertEquals(200, refreshResponse.getStatusLine().getStatusCode());

            {
                Response response = client().performRequest(new Request("GET", "/index/_search"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(10));
            }
            {
                Response response = client().performRequest(new Request("GET", "/index,remote1:index/_search"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(10));
            }
            {
                Response response = client().performRequest(new Request("GET", "/remote1:index/_search"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(0));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(0));
            }

            {
                Response response = client().performRequest(new Request("GET", "/index,remote1:index/_search?scroll=1m"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(10));
                String scrollId = objectPath.evaluate("_scroll_id");
                assertNotNull(scrollId);
                Request scrollRequest = new Request("POST", "/_search/scroll");
                scrollRequest.setJsonEntity("{ \"scroll_id\" : \"" + scrollId + "\" }");
                Response scrollResponse = client().performRequest(scrollRequest);
                assertEquals(200, scrollResponse.getStatusLine().getStatusCode());
                ObjectPath scrollObjectPath = ObjectPath.createFromResponse(scrollResponse);
                assertNull(scrollObjectPath.evaluate("_clusters"));
                assertThat(scrollObjectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(scrollObjectPath.evaluateArraySize("hits.hits"), equalTo(0));
            }

            remoteTransport.close();

            updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", true));

            {
                Response response = client().performRequest(new Request("GET", "/index,remote1:index/_search"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(10));
            }
            {
                Response response = client().performRequest(new Request("GET", "/remote1:index/_search"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(0));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(0));
            }

            {
                Response response = client().performRequest(new Request("GET", "/index,remote1:index/_search?scroll=1m"));
                assertEquals(200, response.getStatusLine().getStatusCode());
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertNotNull(objectPath.evaluate("_clusters"));
                assertThat(objectPath.evaluate("_clusters.total"), equalTo(2));
                assertThat(objectPath.evaluate("_clusters.successful"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.skipped"), equalTo(1));
                assertThat(objectPath.evaluate("_clusters.running"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.partial"), equalTo(0));
                assertThat(objectPath.evaluate("_clusters.failed"), equalTo(0));
                assertThat(objectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(objectPath.evaluateArraySize("hits.hits"), equalTo(10));
                String scrollId = objectPath.evaluate("_scroll_id");
                assertNotNull(scrollId);
                Request scrollRequest = new Request("POST", "/_search/scroll");
                scrollRequest.setJsonEntity("{ \"scroll_id\" : \"" + scrollId + "\" }");
                Response scrollResponse = client().performRequest(scrollRequest);
                assertEquals(200, scrollResponse.getStatusLine().getStatusCode());
                ObjectPath scrollObjectPath = ObjectPath.createFromResponse(scrollResponse);
                assertNull(scrollObjectPath.evaluate("_clusters"));
                assertThat(scrollObjectPath.evaluate("hits.total.value"), equalTo(10));
                assertThat(scrollObjectPath.evaluateArraySize("hits.hits"), equalTo(0));
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
        try (
            MockTransportService remoteTransport = startTransport(
                "node0",
                new CopyOnWriteArrayList<>(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalNode();

            {
                // check that skip_unavailable alone cannot be set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(Collections.singletonMap("skip_unavailable", randomBoolean())));
                ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(
                    responseException.getMessage(),
                    containsString(
                        "Cannot configure setting [cluster.remote.remote1.skip_unavailable] if remote cluster is " + "not enabled."
                    )
                );
            }

            Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put("seeds", remoteNode.getAddress().toString());
            settingsMap.put("skip_unavailable", randomBoolean());
            updateRemoteClusterSettings(settingsMap);

            {
                // check that seeds cannot be reset alone if skip_unavailable is set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(Collections.singletonMap("seeds", null)));
                ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(
                    responseException.getMessage(),
                    containsString(
                        "Cannot configure setting " + "[cluster.remote.remote1.skip_unavailable] if remote cluster is not enabled."
                    )
                );
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
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("POST", "/index,remote1:index/_search"))
            );
            assertThat(exception.getMessage(), containsString("connect_exception"));
        }
        {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("POST", "/remote1:index/_search"))
            );
            assertThat(exception.getMessage(), containsString("connect_exception"));
        }
        {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("POST", "/remote1:index/_search?scroll=1m"))
            );
            assertThat(exception.getMessage(), containsString("connect_exception"));
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

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
