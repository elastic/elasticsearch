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

import com.google.common.collect.Lists;
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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.BaseRestHandler.HeadersCopyClient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class HeadersCopyClientTests extends ElasticsearchTestCase {

    @Test
    public void testAddUsefulHeaders() throws InterruptedException {
        //take the existing headers into account to make sure this test runs with tests.iters>1 as the list is static
        final List<String> headers = Lists.newArrayList(BaseRestHandler.usefulHeaders());
        int iterations = randomIntBetween(1, 5);

        ExecutorService executorService = Executors.newFixedThreadPool(iterations);
        for (int i = 0; i < iterations; i++) {
            int headersCount = randomInt(10);
            final String[] newHeaders = new String[headersCount];
            for (int j = 0; j < headersCount; j++) {
                String usefulHeader = randomRealisticUnicodeOfLengthBetween(1, 30);
                newHeaders[j] = usefulHeader;
                headers.add(usefulHeader);
            }

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    BaseRestHandler.addUsefulHeaders(newHeaders);
                }
            });
        }

        executorService.shutdown();
        assertThat(executorService.awaitTermination(1, TimeUnit.SECONDS), equalTo(true));
        String[] usefulHeaders = BaseRestHandler.usefulHeaders();
        assertThat(usefulHeaders.length, equalTo(headers.size()));

        Arrays.sort(usefulHeaders);
        Collections.sort(headers);
        assertThat(usefulHeaders, equalTo(headers.toArray(new String[headers.size()])));
    }

    @Test
    public void testCopyHeadersRequest() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        SearchRequest searchRequest = Requests.searchRequest();
        putHeaders(searchRequest, existingTransportHeaders);
        assertHeaders(searchRequest, existingTransportHeaders);
        client.search(searchRequest);
        assertHeaders(searchRequest, expectedHeaders);

        GetRequest getRequest = Requests.getRequest("index");
        putHeaders(getRequest, existingTransportHeaders);
        assertHeaders(getRequest, existingTransportHeaders);
        client.get(getRequest);
        assertHeaders(getRequest, expectedHeaders);

        IndexRequest indexRequest = Requests.indexRequest();
        putHeaders(indexRequest, existingTransportHeaders);
        assertHeaders(indexRequest, existingTransportHeaders);
        client.index(indexRequest);
        assertHeaders(indexRequest, expectedHeaders);
    }

    @Test
    public void testCopyHeadersClusterAdminRequest() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest();
        putHeaders(clusterHealthRequest, existingTransportHeaders);
        assertHeaders(clusterHealthRequest, existingTransportHeaders);
        client.admin().cluster().health(clusterHealthRequest);
        assertHeaders(clusterHealthRequest, expectedHeaders);

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        putHeaders(clusterStateRequest, existingTransportHeaders);
        assertHeaders(clusterStateRequest, existingTransportHeaders);
        client.admin().cluster().state(clusterStateRequest);
        assertHeaders(clusterStateRequest, expectedHeaders);

        ClusterStatsRequest clusterStatsRequest = Requests.clusterStatsRequest();
        putHeaders(clusterStatsRequest, existingTransportHeaders);
        assertHeaders(clusterStatsRequest, existingTransportHeaders);
        client.admin().cluster().clusterStats(clusterStatsRequest);
        assertHeaders(clusterStatsRequest, expectedHeaders);
    }

    @Test
    public void testCopyHeadersIndicesAdminRequest() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        CreateIndexRequest createIndexRequest = Requests.createIndexRequest("test");
        putHeaders(createIndexRequest, existingTransportHeaders);
        assertHeaders(createIndexRequest, existingTransportHeaders);
        client.admin().indices().create(createIndexRequest);
        assertHeaders(createIndexRequest, expectedHeaders);

        CloseIndexRequest closeIndexRequest = Requests.closeIndexRequest("test");
        putHeaders(closeIndexRequest, existingTransportHeaders);
        assertHeaders(closeIndexRequest, existingTransportHeaders);
        client.admin().indices().close(closeIndexRequest);
        assertHeaders(closeIndexRequest, expectedHeaders);

        FlushRequest flushRequest = Requests.flushRequest();
        putHeaders(flushRequest, existingTransportHeaders);
        assertHeaders(flushRequest, existingTransportHeaders);
        client.admin().indices().flush(flushRequest);
        assertHeaders(flushRequest, expectedHeaders);
    }

    @Test
    public void testCopyHeadersRequestBuilder() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                client.prepareIndex("index", "type"),
                client.prepareGet("index", "type", "id"),
                client.prepareBulk(),
                client.prepareDelete(),
                client.prepareIndex(),
                client.prepareClearScroll(),
                client.prepareMultiGet(),
                client.prepareBenchStatus()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            putHeaders(requestBuilder.request(), existingTransportHeaders);
            assertHeaders(requestBuilder.request(), existingTransportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
        }
    }

    @Test
    public void testCopyHeadersClusterAdminRequestBuilder() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                client.admin().cluster().prepareNodesInfo(),
                client.admin().cluster().prepareClusterStats(),
                client.admin().cluster().prepareState(),
                client.admin().cluster().prepareCreateSnapshot("repo", "name"),
                client.admin().cluster().prepareHealth(),
                client.admin().cluster().prepareReroute()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            putHeaders(requestBuilder.request(), existingTransportHeaders);
            assertHeaders(requestBuilder.request(), existingTransportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
        }
    }

    @Test
    public void testCopyHeadersIndicesAdminRequestBuilder() {
        Map<String, String> existingTransportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> leftRestHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(leftRestHeaders.keySet());
        usefulRestHeaders.addAll(randomHeaders(randomIntBetween(0, 10), "useful-").keySet());

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(existingTransportHeaders);
        expectedHeaders.putAll(leftRestHeaders);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders), usefulRestHeaders);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                client.admin().indices().prepareValidateQuery(),
                client.admin().indices().prepareCreate("test"),
                client.admin().indices().prepareAliases(),
                client.admin().indices().prepareAnalyze("text"),
                client.admin().indices().prepareDeleteWarmer(),
                client.admin().indices().prepareTypesExists("type"),
                client.admin().indices().prepareClose()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            putHeaders(requestBuilder.request(), existingTransportHeaders);
            assertHeaders(requestBuilder.request(), existingTransportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
        }
    }

    private static Map<String, String> randomHeaders(int count) {
        return randomHeaders(count, "header-");
    }

    private static Map<String, String> randomHeaders(int count, String prefix) {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < count; i++) {
            headers.put(prefix + randomInt(30), randomRealisticUnicodeOfLengthBetween(1, 20));
        }
        return headers;
    }

    private static Map<String, String> randomHeadersFrom(Map<String, String> headers) {
        Map<String, String> newHeaders = new HashMap<>();
        if (headers.isEmpty()) {
            return newHeaders;
        }
        int i = randomInt(headers.size() - 1);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (randomInt(i) == 0) {
                newHeaders.put(entry.getKey(), entry.getValue());
            }
        }
        return newHeaders;
    }

    private static Client client(Client noOpClient, RestRequest restRequest, Set<String> usefulRestHeaders) {
        if (usefulRestHeaders.isEmpty() && randomBoolean()) {
            return noOpClient;
        }
        return new HeadersCopyClient(noOpClient, restRequest, usefulRestHeaders.toArray(new String[usefulRestHeaders.size()]));
    }

    private static void putHeaders(ActionRequest<?> request, Map<String, String> headers) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.putHeader(header.getKey(), header.getValue());
        }
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
