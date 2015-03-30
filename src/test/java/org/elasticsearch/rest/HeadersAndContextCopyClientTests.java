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

import com.google.common.collect.Maps;
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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.*;

public class HeadersAndContextCopyClientTests extends ElasticsearchTestCase {

    @Test
    public void testRegisterRelevantHeaders() throws InterruptedException {

        final RestController restController = new RestController(ImmutableSettings.EMPTY);

        int iterations = randomIntBetween(1, 5);

        Set<String> headers = new HashSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(iterations);
        for (int i = 0; i < iterations; i++) {
            int headersCount = randomInt(10);
            final Set<String> newHeaders = new HashSet<>();
            for (int j = 0; j < headersCount; j++) {
                String usefulHeader = randomRealisticUnicodeOfLengthBetween(1, 30);
                newHeaders.add(usefulHeader);
            }
            headers.addAll(newHeaders);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    restController.registerRelevantHeaders(newHeaders.toArray(new String[newHeaders.size()]));
                }
            });
        }

        executorService.shutdown();
        assertThat(executorService.awaitTermination(1, TimeUnit.SECONDS), equalTo(true));
        String[] relevantHeaders = restController.relevantHeaders().toArray(new String[restController.relevantHeaders().size()]);
        assertThat(relevantHeaders.length, equalTo(headers.size()));

        Arrays.sort(relevantHeaders);
        String[] headersArray = new String[headers.size()];
        headersArray = headers.toArray(headersArray);
        Arrays.sort(headersArray);
        assertThat(relevantHeaders, equalTo(headersArray));
    }

    @Test
    public void testCopyHeadersRequest() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, restContext), usefulRestHeaders);

        SearchRequest searchRequest = Requests.searchRequest();
        putHeaders(searchRequest, transportHeaders);
        putContext(searchRequest, transportContext);
        assertHeaders(searchRequest, transportHeaders);
        client.search(searchRequest);
        assertHeaders(searchRequest, expectedHeaders);
        assertContext(searchRequest, expectedContext);

        GetRequest getRequest = Requests.getRequest("index");
        putHeaders(getRequest, transportHeaders);
        putContext(getRequest, transportContext);
        assertHeaders(getRequest, transportHeaders);
        client.get(getRequest);
        assertHeaders(getRequest, expectedHeaders);
        assertContext(getRequest, expectedContext);

        IndexRequest indexRequest = Requests.indexRequest();
        putHeaders(indexRequest, transportHeaders);
        putContext(indexRequest, transportContext);
        assertHeaders(indexRequest, transportHeaders);
        client.index(indexRequest);
        assertHeaders(indexRequest, expectedHeaders);
        assertContext(indexRequest, expectedContext);
    }

    @Test
    public void testCopyHeadersClusterAdminRequest() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, expectedContext), usefulRestHeaders);

        ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest();
        putHeaders(clusterHealthRequest, transportHeaders);
        putContext(clusterHealthRequest, transportContext);
        assertHeaders(clusterHealthRequest, transportHeaders);
        client.admin().cluster().health(clusterHealthRequest);
        assertHeaders(clusterHealthRequest, expectedHeaders);
        assertContext(clusterHealthRequest, expectedContext);

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        putHeaders(clusterStateRequest, transportHeaders);
        putContext(clusterStateRequest, transportContext);
        assertHeaders(clusterStateRequest, transportHeaders);
        client.admin().cluster().state(clusterStateRequest);
        assertHeaders(clusterStateRequest, expectedHeaders);
        assertContext(clusterStateRequest, expectedContext);

        ClusterStatsRequest clusterStatsRequest = Requests.clusterStatsRequest();
        putHeaders(clusterStatsRequest, transportHeaders);
        putContext(clusterStatsRequest, transportContext);
        assertHeaders(clusterStatsRequest, transportHeaders);
        client.admin().cluster().clusterStats(clusterStatsRequest);
        assertHeaders(clusterStatsRequest, expectedHeaders);
        assertContext(clusterStatsRequest, expectedContext);
    }

    @Test
    public void testCopyHeadersIndicesAdminRequest() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, restContext), usefulRestHeaders);

        CreateIndexRequest createIndexRequest = Requests.createIndexRequest("test");
        putHeaders(createIndexRequest, transportHeaders);
        putContext(createIndexRequest, transportContext);
        assertHeaders(createIndexRequest, transportHeaders);
        client.admin().indices().create(createIndexRequest);
        assertHeaders(createIndexRequest, expectedHeaders);
        assertContext(createIndexRequest, expectedContext);

        CloseIndexRequest closeIndexRequest = Requests.closeIndexRequest("test");
        putHeaders(closeIndexRequest, transportHeaders);
        putContext(closeIndexRequest, transportContext);
        assertHeaders(closeIndexRequest, transportHeaders);
        client.admin().indices().close(closeIndexRequest);
        assertHeaders(closeIndexRequest, expectedHeaders);
        assertContext(closeIndexRequest, expectedContext);

        FlushRequest flushRequest = Requests.flushRequest();
        putHeaders(flushRequest, transportHeaders);
        putContext(flushRequest, transportContext);
        assertHeaders(flushRequest, transportHeaders);
        client.admin().indices().flush(flushRequest);
        assertHeaders(flushRequest, expectedHeaders);
        assertContext(flushRequest, expectedContext);
    }

    @Test
    public void testCopyHeadersRequestBuilder() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, restContext), usefulRestHeaders);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                client.prepareIndex("index", "type"),
                client.prepareGet("index", "type", "id"),
                client.prepareBulk(),
                client.prepareDelete(),
                client.prepareIndex(),
                client.prepareClearScroll(),
                client.prepareMultiGet(),
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            putHeaders(requestBuilder.request(), transportHeaders);
            putContext(requestBuilder.request(), transportContext);
            assertHeaders(requestBuilder.request(), transportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
            assertContext(requestBuilder.request(), expectedContext);
        }
    }

    @Test
    public void testCopyHeadersClusterAdminRequestBuilder() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, restContext), usefulRestHeaders);

        ActionRequestBuilder requestBuilders [] = new ActionRequestBuilder[] {
                client.admin().cluster().prepareNodesInfo(),
                client.admin().cluster().prepareClusterStats(),
                client.admin().cluster().prepareState(),
                client.admin().cluster().prepareCreateSnapshot("repo", "name"),
                client.admin().cluster().prepareHealth(),
                client.admin().cluster().prepareReroute()
        };

        for (ActionRequestBuilder requestBuilder : requestBuilders) {
            putHeaders(requestBuilder.request(), transportHeaders);
            putContext(requestBuilder.request(), transportContext);
            assertHeaders(requestBuilder.request(), transportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
            assertContext(requestBuilder.request(), expectedContext);
        }
    }

    @Test
    public void testCopyHeadersIndicesAdminRequestBuilder() {
        Map<String, String> transportHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> restHeaders = randomHeaders(randomIntBetween(0, 10));
        Map<String, String> copiedHeaders = randomHeadersFrom(restHeaders);
        Set<String> usefulRestHeaders = new HashSet<>(copiedHeaders.keySet());
        usefulRestHeaders.addAll(randomMap(randomIntBetween(0, 10), "useful-").keySet());
        Map<String, String> restContext = randomContext(randomIntBetween(0, 10));
        Map<String, String> transportContext = Maps.difference(randomContext(randomIntBetween(0, 10)), restContext).entriesOnlyOnLeft();

        HashMap<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.putAll(transportHeaders);
        expectedHeaders.putAll(copiedHeaders);

        Map<String, String> expectedContext = new HashMap<>();
        expectedContext.putAll(transportContext);
        expectedContext.putAll(restContext);

        Client client = client(new NoOpClient(), new FakeRestRequest(restHeaders, restContext), usefulRestHeaders);

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
            putHeaders(requestBuilder.request(), transportHeaders);
            putContext(requestBuilder.request(), transportContext);
            assertHeaders(requestBuilder.request(), transportHeaders);
            requestBuilder.get();
            assertHeaders(requestBuilder.request(), expectedHeaders);
            assertContext(requestBuilder.request(), expectedContext);
        }
    }

    private static Map<String, String> randomHeaders(int count) {
        return randomMap(count, "header-");
    }

    private static Map<String, String> randomContext(int count) {
        return randomMap(count, "context-");
    }

    private static Map<String, String> randomMap(int count, String prefix) {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < count; i++) {
            headers.put(prefix + randomInt(30), randomAsciiOfLength(10));
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
        return new BaseRestHandler.HeadersAndContextCopyClient(noOpClient, restRequest, usefulRestHeaders);
    }

    private static void putHeaders(ActionRequest<?> request, Map<String, String> headers) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.putHeader(header.getKey(), header.getValue());
        }
    }

    private static void putContext(ActionRequest<?> request, Map<String, String> context) {
        for (Map.Entry<String, String> header : context.entrySet()) {
            request.putInContext(header.getKey(), header.getValue());
        }
    }

    private static void assertHeaders(ActionRequest<?> request, Map<String, String> headers) {
        if (headers.size() == 0) {
            assertThat(request.getHeaders() == null || request.getHeaders().size() == 0, equalTo(true));
        } else {
            assertThat(request.getHeaders(), notNullValue());
            assertThat(request.getHeaders().size(), equalTo(headers.size()));
            for (String key : request.getHeaders()) {
                assertThat(headers.get(key), equalTo(request.getHeader(key)));
            }
        }
    }

    private static void assertContext(ActionRequest<?> request, Map<String, String> context) {
        if (context.size() == 0) {
            assertThat(request.isContextEmpty(), is(true));
        } else {
            ImmutableOpenMap map = request.getContext();
            assertThat(map, notNullValue());
            assertThat(map.size(), equalTo(context.size()));
            for (Object key : map.keys()) {
                assertThat(context.get(key), equalTo(request.getFromContext(key)));
            }
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
