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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class HeadersCopyClientTests extends ElasticsearchTestCase {

    @Test
    public void testCopyHeadersRequest() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);
        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        SearchRequest searchRequest = Requests.searchRequest();
        searchRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(searchRequest, existingHeaders);
        headersCopyClient.search(searchRequest);
        assertHeaders(searchRequest, resultingHeaders);

        GetRequest getRequest = Requests.getRequest("index");
        getRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(getRequest, existingHeaders);
        headersCopyClient.get(getRequest);
        assertHeaders(getRequest, resultingHeaders);

        IndexRequest indexRequest = Requests.indexRequest();
        indexRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(indexRequest, existingHeaders);
        headersCopyClient.index(indexRequest);
        assertHeaders(indexRequest, resultingHeaders);
    }

    @Test
    public void testCopyHeadersClusterAdminRequest() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);
        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest();
        clusterHealthRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(clusterHealthRequest, existingHeaders);
        headersCopyClient.admin().cluster().health(clusterHealthRequest);
        assertHeaders(clusterHealthRequest, resultingHeaders);

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        clusterStateRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(clusterStateRequest, existingHeaders);
        headersCopyClient.admin().cluster().state(clusterStateRequest);
        assertHeaders(clusterStateRequest, resultingHeaders);

        ClusterStatsRequest clusterStatsRequest = Requests.clusterStatsRequest();
        clusterStatsRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(clusterStatsRequest, existingHeaders);
        headersCopyClient.admin().cluster().clusterStats(clusterStatsRequest);
        assertHeaders(clusterStatsRequest, resultingHeaders);
    }

    @Test
    public void testCopyHeadersIndicesAdminRequest() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);
        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        CreateIndexRequest createIndexRequest = Requests.createIndexRequest("test");
        createIndexRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(createIndexRequest, existingHeaders);
        headersCopyClient.admin().indices().create(createIndexRequest);
        assertHeaders(createIndexRequest, resultingHeaders);

        CloseIndexRequest closeIndexRequest = Requests.closeIndexRequest("test");
        closeIndexRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(closeIndexRequest, existingHeaders);
        headersCopyClient.admin().indices().close(closeIndexRequest);
        assertHeaders(closeIndexRequest, resultingHeaders);

        FlushRequest flushRequest = Requests.flushRequest();
        flushRequest.putHeaders(existingHeaders.entrySet());
        assertHeaders(flushRequest, existingHeaders);
        headersCopyClient.admin().indices().flush(flushRequest);
        assertHeaders(flushRequest, resultingHeaders);
    }

    @Test
    public void testCopyHeadersRequestBuilder() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);

        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                headersCopyClient.prepareIndex("index", "type"),
                headersCopyClient.prepareGet("index", "type", "id"),
                headersCopyClient.prepareBulk(),
                headersCopyClient.prepareDelete(),
                headersCopyClient.prepareIndex(),
                headersCopyClient.prepareClearScroll(),
                headersCopyClient.prepareMultiGet(),
                headersCopyClient.prepareBenchStatus()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            requestBuilder.request().putHeaders(existingHeaders.entrySet());
            assertHeaders(requestBuilder.request(), existingHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), resultingHeaders);
        }
    }

    @Test
    public void testCopyHeadersClusterAdminRequestBuilder() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);

        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                headersCopyClient.admin().cluster().prepareNodesInfo(),
                headersCopyClient.admin().cluster().prepareClusterStats(),
                headersCopyClient.admin().cluster().prepareState(),
                headersCopyClient.admin().cluster().prepareCreateSnapshot("repo", "name"),
                headersCopyClient.admin().cluster().prepareHealth(),
                headersCopyClient.admin().cluster().prepareReroute()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            requestBuilder.request().putHeaders(existingHeaders.entrySet());
            assertHeaders(requestBuilder.request(), existingHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), resultingHeaders);
        }
    }

    @Test
    public void testCopyHeadersIndicesAdminRequestBuilder() {
        Map<String, String> existingHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> newHeaders = randomHeaders(randomIntBetween(0, 10));

        HashMap<String, String> resultingHeaders = new HashMap<>();
        resultingHeaders.putAll(existingHeaders);
        resultingHeaders.putAll(newHeaders);

        RestRequest restRequest = new FakeRestRequest(newHeaders);

        NoOpClient noOpClient = new NoOpClient();
        HeadersCopyClient headersCopyClient = new HeadersCopyClient(noOpClient, restRequest);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                headersCopyClient.admin().indices().prepareValidateQuery(),
                headersCopyClient.admin().indices().prepareCreate("test"),
                headersCopyClient.admin().indices().prepareAliases(),
                headersCopyClient.admin().indices().prepareAnalyze("text"),
                headersCopyClient.admin().indices().prepareDeleteWarmer(),
                headersCopyClient.admin().indices().prepareTypesExists("type"),
                headersCopyClient.admin().indices().prepareClose()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            requestBuilder.request().putHeaders(existingHeaders.entrySet());
            assertHeaders(requestBuilder.request(), existingHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), resultingHeaders);
        }
    }

    private static Map<String, String> randomHeaders(int count) {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < count; i++) {
            headers.put("header-" + randomInt(30), randomRealisticUnicodeOfLengthBetween(1, 20));
        }
        return headers;
    }

    private static void assertHeaders(ActionRequest<?> request, Map<String, String> headers) {
        if (headers.size() == 0) {
            assertThat(request.getHeaders() == null || request.getHeaders().size() == 0, equalTo(true));
        } else {
            assertThat(request.getHeaders(), notNullValue());
            assertThat(request.getHeaders().size(), equalTo(headers.size()));
            for (Map.Entry<String, Object> entry : request.getHeaders().entrySet()) {
                assertThat(headers.get(entry.getKey()), equalTo(entry.getValue()));
            }
        }
    }

    private static class FakeRestRequest extends RestRequest {

        private final Map<String, String> headers;

        private FakeRestRequest(Map<String, String> headers) {
            this.headers = headers;
        }

        @Override
        public Method method() {
            return null;
        }

        @Override
        public String uri() {
            return null;
        }

        @Override
        public String rawPath() {
            return null;
        }

        @Override
        public boolean hasContent() {
            return false;
        }

        @Override
        public boolean contentUnsafe() {
            return false;
        }

        @Override
        public BytesReference content() {
            return null;
        }

        @Override
        public String header(String name) {
            return headers.get(name);
        }

        @Override
        public Iterable<Map.Entry<String, String>> headers() {
            return headers.entrySet();
        }

        @Override
        public boolean hasParam(String key) {
            return false;
        }

        @Override
        public String param(String key) {
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            return null;
        }

        @Override
        public Map<String, String> params() {
            return null;
        }
    }

    private static class NoOpClient extends AbstractClient implements AdminClient {

        @Override
        public AdminClient admin() {
            return this;
        }

        @Override
        public Settings settings() {
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
            listener.onResponse(null);
        }

        @Override
        public ThreadPool threadPool() {
            return null;
        }

        @Override
        public void close() throws ElasticsearchException {

        }

        @Override
        public ClusterAdminClient cluster() {
            return new AbstractClusterAdminClient() {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request) {
                    return null;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request, ActionListener<Response> listener) {
                    listener.onResponse(null);
                }

                @Override
                public ThreadPool threadPool() {
                    return null;
                }
            };
        }

        @Override
        public IndicesAdminClient indices() {
            return new AbstractIndicesAdminClient() {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
                    return null;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
                    listener.onResponse(null);
                }

                @Override
                public ThreadPool threadPool() {
                    return null;
                }
            };
        }
    }
}
