/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.remoteaccess;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RemoteAccessHeadersRestIT extends SecurityOnTrialLicenseRestTestCase {
    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testRemoteAccessHeadersSent() throws Exception {
        final String remoteNodeName = "remoteNode";
        final String clusterCredential = randomAlphaOfLengthBetween(42, 100);
        final ConcurrentHashMap<String, Map<String, String>> capturedHeadersByActionName = new ConcurrentHashMap<>();
        try (
            MockTransportService remoteTransport = startTransport(
                remoteNodeName,
                new CopyOnWriteArrayList<>(),
                threadPool,
                capturedHeadersByActionName
            )
        ) {
            final DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();
            updateRemoteClusterSettings(Map.of("authorization", clusterCredential, "seeds", remoteNode.getAddress().toString()));
            final Request searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");
            assertOK(client().performRequest(searchRequest));
            assertThat(
                capturedHeadersByActionName.get(SearchAction.NAME).keySet(),
                contains(
                    RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY,
                    SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER
                )
            );
        }
    }

    private static MockTransportService startTransport(
        final String nodeName,
        final List<DiscoveryNode> knownNodes,
        final ThreadPool threadPool,
        final Map<String, Map<String, String>> capturedHeadersByActionName
    ) {
        boolean success = false;
        final Settings settings = Settings.builder().put("node.name", nodeName).build();
        final ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        final MockTransportService newService = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
        try {
            newService.registerRequestHandler(
                ClusterStateAction.NAME,
                ThreadPool.Names.SAME,
                ClusterStateRequest::new,
                (request, channel, task) -> {
                    channel.sendResponse(new ClusterStateResponse(clusterName, ClusterState.builder(clusterName).build(), false));
                }
            );
            newService.registerRequestHandler(
                ClusterSearchShardsAction.NAME,
                ThreadPool.Names.SAME,
                ClusterSearchShardsRequest::new,
                (request, channel, task) -> {
                    capturedHeadersByActionName.put(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()));
                    channel.sendResponse(
                        new ClusterSearchShardsResponse(
                            new ClusterSearchShardsGroup[0],
                            knownNodes.toArray(new DiscoveryNode[0]),
                            Collections.emptyMap()
                        )
                    );
                }
            );
            newService.registerRequestHandler(SearchAction.NAME, ThreadPool.Names.SAME, SearchRequest::new, (request, channel, task) -> {
                capturedHeadersByActionName.put(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()));
                channel.sendResponse(
                    new SearchResponse(
                        new InternalSearchResponse(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                            InternalAggregations.EMPTY,
                            null,
                            null,
                            false,
                            null,
                            1
                        ),
                        null,
                        1,
                        1,
                        0,
                        100,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY
                    )
                );
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

    private static void updateRemoteClusterSettings(final Map<String, Object> settings) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setEntity(buildUpdateSettingsRequestBody(settings));
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static HttpEntity buildUpdateSettingsRequestBody(final Map<String, Object> settings) throws IOException {
        final String requestBody;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("persistent");
                {
                    builder.startObject("cluster.remote.my_remote_cluster");
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

}
