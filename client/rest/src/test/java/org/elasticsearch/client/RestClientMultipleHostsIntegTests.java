/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpHost;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.client.RestClientTestUtil.getAllStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.randomErrorNoRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomOkStatusCode;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test to check interaction between {@link RestClient} and {@link org.apache.http.client.HttpClient}.
 * Works against real http servers, multiple hosts. Also tests failover by randomly shutting down hosts.
 */
public class RestClientMultipleHostsIntegTests extends RestClientTestCase {

    private static WaitForCancelHandler waitForCancelHandler;
    private static HttpServer[] httpServers;
    private static HttpHost[] httpHosts;
    private static boolean stoppedFirstHost = false;
    private static String pathPrefixWithoutLeadingSlash;
    private static String pathPrefix;
    private static RestClient restClient;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        if (randomBoolean()) {
            pathPrefixWithoutLeadingSlash = "testPathPrefix/" + randomAsciiLettersOfLengthBetween(1, 5);
            pathPrefix = "/" + pathPrefixWithoutLeadingSlash;
        } else {
            pathPrefix = pathPrefixWithoutLeadingSlash = "";
        }
        int numHttpServers = randomIntBetween(2, 4);
        httpServers = new HttpServer[numHttpServers];
        httpHosts = new HttpHost[numHttpServers];
        waitForCancelHandler = new WaitForCancelHandler();
        for (int i = 0; i < numHttpServers; i++) {
            HttpServer httpServer = createHttpServer();
            httpServers[i] = httpServer;
            httpHosts[i] = new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort());
        }
        restClient = buildRestClient(NodeSelector.ANY);
    }

    private static RestClient buildRestClient(NodeSelector nodeSelector) {
        return buildRestClient(nodeSelector, null);
    }

    private static RestClient buildRestClient(NodeSelector nodeSelector, RestClient.FailureListener failureListener) {
        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        if (pathPrefix.length() > 0) {
            restClientBuilder.setPathPrefix((randomBoolean() ? "/" : "") + pathPrefixWithoutLeadingSlash);
        }
        if (failureListener != null) {
            restClientBuilder.setFailureListener(failureListener);
        }
        restClientBuilder.setNodeSelector(nodeSelector);
        return restClientBuilder.build();
    }

    private static HttpServer createHttpServer() throws Exception {
        HttpServer httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        // returns a different status code depending on the path
        for (int statusCode : getAllStatusCodes()) {
            httpServer.createContext(pathPrefix + "/" + statusCode, new ResponseHandler(statusCode));
        }
        httpServer.createContext(pathPrefix + "/20bytes", new ResponseHandlerWithContent());
        httpServer.createContext(pathPrefix + "/wait", waitForCancelHandler);
        return httpServer;
    }

    private static WaitForCancelHandler resetWaitHandlers() {
        WaitForCancelHandler handler = new WaitForCancelHandler();
        for (HttpServer httpServer : httpServers) {
            httpServer.removeContext(pathPrefix + "/wait");
            httpServer.createContext(pathPrefix + "/wait", handler);
        }
        return handler;
    }

    private static class WaitForCancelHandler implements HttpHandler {
        private final CountDownLatch requestCameInLatch = new CountDownLatch(1);
        private final CountDownLatch cancelHandlerLatch = new CountDownLatch(1);

        void cancelDone() {
            cancelHandlerLatch.countDown();
        }

        void awaitRequest() throws InterruptedException {
            requestCameInLatch.await();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            requestCameInLatch.countDown();
            try {
                cancelHandlerLatch.await();
            } catch (InterruptedException ignore) {} finally {
                exchange.sendResponseHeaders(200, 0);
                exchange.close();
            }
        }
    }

    private static class ResponseHandler implements HttpHandler {
        private final int statusCode;

        ResponseHandler(int statusCode) {
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.getRequestBody().close();
            httpExchange.sendResponseHeaders(statusCode, -1);
            httpExchange.close();
        }
    }

    private static class ResponseHandlerWithContent implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            byte[] body = "01234567890123456789".getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = httpExchange.getResponseBody()) {
                out.write(body);
            }
            httpExchange.close();
        }
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        restClient.close();
        restClient = null;
        for (HttpServer httpServer : httpServers) {
            httpServer.stop(0);
        }
        httpServers = null;
    }

    @Before
    public void stopRandomHost() {
        // verify that shutting down some hosts doesn't matter as long as one working host is left behind
        if (httpServers.length > 1 && randomBoolean()) {
            List<HttpServer> updatedHttpServers = new ArrayList<>(httpServers.length - 1);
            int nodeIndex = randomIntBetween(0, httpServers.length - 1);
            if (0 == nodeIndex) {
                stoppedFirstHost = true;
            }
            for (int i = 0; i < httpServers.length; i++) {
                HttpServer httpServer = httpServers[i];
                if (i == nodeIndex) {
                    httpServer.stop(0);
                } else {
                    updatedHttpServers.add(httpServer);
                }
            }
            httpServers = updatedHttpServers.toArray(new HttpServer[0]);
        }
    }

    public void testSyncRequests() throws IOException {
        int numRequests = randomIntBetween(5, 20);
        for (int i = 0; i < numRequests; i++) {
            final String method = RestClientTestUtil.randomHttpMethod(getRandom());
            // we don't test status codes that are subject to retries as they interfere with hosts being stopped
            final int statusCode = randomBoolean() ? randomOkStatusCode(getRandom()) : randomErrorNoRetryStatusCode(getRandom());
            Response response;
            try {
                response = restClient.performRequest(new Request(method, "/" + statusCode));
            } catch (ResponseException responseException) {
                response = responseException.getResponse();
            }
            assertEquals(method, response.getRequestLine().getMethod());
            assertEquals(statusCode, response.getStatusLine().getStatusCode());
            assertEquals((pathPrefix.length() > 0 ? pathPrefix : "") + "/" + statusCode, response.getRequestLine().getUri());
        }
    }

    public void testAsyncRequests() throws Exception {
        int numRequests = randomIntBetween(5, 20);
        final CountDownLatch latch = new CountDownLatch(numRequests);
        final List<TestResponse> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            final String method = RestClientTestUtil.randomHttpMethod(getRandom());
            // we don't test status codes that are subject to retries as they interfere with hosts being stopped
            final int statusCode = randomBoolean() ? randomOkStatusCode(getRandom()) : randomErrorNoRetryStatusCode(getRandom());
            restClient.performRequestAsync(new Request(method, "/" + statusCode), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    responses.add(new TestResponse(method, statusCode, response));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    responses.add(new TestResponse(method, statusCode, exception));
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(numRequests, responses.size());
        for (TestResponse testResponse : responses) {
            Response response = testResponse.getResponse();
            assertEquals(testResponse.method, response.getRequestLine().getMethod());
            assertEquals(testResponse.statusCode, response.getStatusLine().getStatusCode());
            assertEquals((pathPrefix.length() > 0 ? pathPrefix : "") + "/" + testResponse.statusCode, response.getRequestLine().getUri());
        }
    }

    @Ignore("https://github.com/elastic/elasticsearch/issues/45577")
    public void testCancelAsyncRequests() throws Exception {
        int numRequests = randomIntBetween(5, 20);
        final List<Response> responses = new CopyOnWriteArrayList<>();
        final List<Exception> exceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            waitForCancelHandler = resetWaitHandlers();
            Cancellable cancellable = restClient.performRequestAsync(new Request("GET", "/wait"), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    responses.add(response);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    exceptions.add(exception);
                    latch.countDown();
                }
            });
            if (randomBoolean()) {
                // we wait for the request to get to the server-side otherwise we almost always cancel
                // the request artificially on the client-side before even sending it
                waitForCancelHandler.awaitRequest();
            }
            cancellable.cancel();
            waitForCancelHandler.cancelDone();
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
        assertEquals(0, responses.size());
        assertEquals(numRequests, exceptions.size());
        for (Exception exception : exceptions) {
            assertThat(exception, instanceOf(CancellationException.class));
        }
    }

    /**
     * Test host selector against a real server <strong>and</strong>
     * test what happens after calling
     */
    public void testNodeSelector() throws Exception {
        try (RestClient restClient = buildRestClient(firstPositionNodeSelector())) {
            Request request = new Request("GET", "/200");
            int rounds = between(1, 10);
            for (int i = 0; i < rounds; i++) {
                /*
                 * Run the request more than once to verify that the
                 * NodeSelector overrides the round robin behavior.
                 */
                if (stoppedFirstHost) {
                    try {
                        RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
                        fail("expected to fail to connect");
                    } catch (ConnectException e) {
                        // Windows isn't consistent here. Sometimes the message is even null!
                        if (false == System.getProperty("os.name").startsWith("Windows")) {
                            assertEquals("Connection refused", e.getMessage());
                        }
                    }
                } else {
                    Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
                    assertEquals(httpHosts[0], response.getHost());
                }
            }
        }
    }

    @Ignore("https://github.com/elastic/elasticsearch/issues/87314")
    public void testNonRetryableException() throws Exception {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setHttpAsyncResponseConsumerFactory(
            // Limit to very short responses to trigger a ContentTooLongException
            () -> new HeapBufferedAsyncResponseConsumer(10)
        );

        AtomicInteger failureCount = new AtomicInteger();
        RestClient client = buildRestClient(NodeSelector.ANY, new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                failureCount.incrementAndGet();
            }
        });

        failureCount.set(0);
        Request request = new Request("POST", "/20bytes");
        request.setOptions(options);
        try {
            RestClientSingleHostTests.performRequestSyncOrAsync(client, request);
            fail("Request should not succeed");
        } catch (IOException e) {
            assertEquals(stoppedFirstHost ? 2 : 1, failureCount.intValue());
        }

        client.close();
    }

    private static class TestResponse {
        private final String method;
        private final int statusCode;
        private final Object response;

        TestResponse(String method, int statusCode, Object response) {
            this.method = method;
            this.statusCode = statusCode;
            this.response = response;
        }

        Response getResponse() {
            if (response instanceof Response) {
                return (Response) response;
            }
            if (response instanceof ResponseException) {
                return ((ResponseException) response).getResponse();
            }
            throw new AssertionError("unexpected response " + response.getClass());
        }
    }

    private NodeSelector firstPositionNodeSelector() {
        return nodes -> {
            for (Iterator<Node> itr = nodes.iterator(); itr.hasNext();) {
                if (httpHosts[0] != itr.next().getHost()) {
                    itr.remove();
                }
            }
        };
    }
}
