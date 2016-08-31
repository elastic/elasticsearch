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

package org.elasticsearch.client;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.RestClientTestUtil.getAllStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.getHttpMethods;
import static org.elasticsearch.client.RestClientTestUtil.randomStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test to check interaction between {@link RestClient} and {@link org.apache.http.client.HttpClient}.
 * Works against a real http server, one single host.
 */
//animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
@IgnoreJRERequirement
public class RestClientIntegTests extends RestClientTestCase {

    private static HttpServer httpServer;
    private static RestClient restClient;
    private static Header[] defaultHeaders;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        //returns a different status code depending on the path
        for (int statusCode : getAllStatusCodes()) {
            createStatusCodeContext(httpServer, statusCode);
        }
        int numHeaders = randomIntBetween(0, 5);
        defaultHeaders = generateHeaders("Header-default", "Header-array", numHeaders);
        restClient = RestClient.builder(new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort()))
                .setDefaultHeaders(defaultHeaders).build();
    }

    private static void createStatusCodeContext(HttpServer httpServer, final int statusCode) {
        httpServer.createContext("/" + statusCode, new ResponseHandler(statusCode));
    }

    //animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
    @IgnoreJRERequirement
    private static class ResponseHandler implements HttpHandler {
        private final int statusCode;

        ResponseHandler(int statusCode) {
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder body = new StringBuilder();
            try (InputStreamReader reader = new InputStreamReader(httpExchange.getRequestBody(), Consts.UTF_8)) {
                char[] buffer = new char[256];
                int read;
                while ((read = reader.read(buffer)) != -1) {
                    body.append(buffer, 0, read);
                }
            }
            Headers requestHeaders = httpExchange.getRequestHeaders();
            Headers responseHeaders = httpExchange.getResponseHeaders();
            for (Map.Entry<String, List<String>> header : requestHeaders.entrySet()) {
                responseHeaders.put(header.getKey(), header.getValue());
            }
            httpExchange.getRequestBody().close();
            httpExchange.sendResponseHeaders(statusCode, body.length() == 0 ? -1 : body.length());
            if (body.length() > 0) {
                try (OutputStream out = httpExchange.getResponseBody()) {
                    out.write(body.toString().getBytes(Consts.UTF_8));
                }
            }
            httpExchange.close();
        }
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        restClient.close();
        restClient = null;
        httpServer.stop(0);
        httpServer = null;
    }

    /**
     * End to end test for headers. We test it explicitly against a real http client as there are different ways
     * to set/add headers to the {@link org.apache.http.client.HttpClient}.
     * Exercises the test http server ability to send back whatever headers it received.
     */
    public void testHeaders() throws IOException {
        for (String method : getHttpMethods()) {
            final Set<String> standardHeaders = new HashSet<>(Arrays.asList("Connection", "Host", "User-agent", "Date"));
            if (method.equals("HEAD") == false) {
                standardHeaders.add("Content-length");
            }

            final int numHeaders = randomIntBetween(1, 5);
            final Header[] headers = generateHeaders("Header", "Header-array", numHeaders);
            final Map<String, List<String>> expectedHeaders = new HashMap<>();

            addHeaders(expectedHeaders, defaultHeaders, headers);

            final int statusCode = randomStatusCode(getRandom());
            Response esResponse;
            try {
                esResponse = restClient.performRequest(method, "/" + statusCode, Collections.<String, String>emptyMap(), headers);
            } catch(ResponseException e) {
                esResponse = e.getResponse();
            }
            assertThat(esResponse.getStatusLine().getStatusCode(), equalTo(statusCode));
            for (final Header responseHeader : esResponse.getHeaders()) {
                final String name = responseHeader.getName();
                final String value = responseHeader.getValue();
                if (name.startsWith("Header")) {
                    final List<String> values = expectedHeaders.get(name);
                    assertNotNull("found response header [" + name + "] that wasn't originally sent: " + value, values);
                    assertTrue("found incorrect response header [" + name + "]: " + value, values.remove(value));

                    // we've collected them all
                    if (values.isEmpty()) {
                        expectedHeaders.remove(name);
                    }
                } else {
                    assertTrue("unknown header was returned " + name, standardHeaders.remove(name));
                }
            }
            assertTrue("some headers that were sent weren't returned: " + expectedHeaders, expectedHeaders.isEmpty());
            assertTrue("some expected standard headers weren't returned: " + standardHeaders, standardHeaders.isEmpty());
        }
    }

    /**
     * End to end test for delete with body. We test it explicitly as it is not supported
     * out of the box by {@link org.apache.http.client.HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testDeleteWithBody() throws IOException {
        bodyTest("DELETE");
    }

    /**
     * End to end test for get with body. We test it explicitly as it is not supported
     * out of the box by {@link org.apache.http.client.HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testGetWithBody() throws IOException {
        bodyTest("GET");
    }

    /**
     * Ensure that pathPrefix works as expected.
     */
    public void testPathPrefix() throws IOException {
        // guarantee no other test setup collides with this one and lets it sneak through
        final String uniqueContextSuffix = "/testPathPrefix";
        final String pathPrefix = "base/" + randomAsciiOfLengthBetween(1, 5) + "/";
        final int statusCode = randomStatusCode(getRandom());

        final HttpContext context =
            httpServer.createContext("/" + pathPrefix + statusCode + uniqueContextSuffix, new ResponseHandler(statusCode));

        try (final RestClient client =
                RestClient.builder(new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort()))
                    .setPathPrefix((randomBoolean() ? "/" : "") + pathPrefix).build()) {

            for (final String method : getHttpMethods()) {
                Response esResponse;
                try {
                    esResponse = client.performRequest(method, "/" + statusCode + uniqueContextSuffix);
                } catch(ResponseException e) {
                    esResponse = e.getResponse();
                }

                assertThat(esResponse.getRequestLine().getUri(), equalTo("/" + pathPrefix + statusCode + uniqueContextSuffix));
                assertThat(esResponse.getStatusLine().getStatusCode(), equalTo(statusCode));
            }
        } finally {
            httpServer.removeContext(context);
        }
    }

    private void bodyTest(String method) throws IOException {
        String requestBody = "{ \"field\": \"value\" }";
        StringEntity entity = new StringEntity(requestBody);
        int statusCode = randomStatusCode(getRandom());
        Response esResponse;
        try {
            esResponse = restClient.performRequest(method, "/" + statusCode, Collections.<String, String>emptyMap(), entity);
        } catch(ResponseException e) {
            esResponse = e.getResponse();
        }
        assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
        assertEquals(requestBody, EntityUtils.toString(esResponse.getEntity()));
    }

    public void testAsyncRequests() throws Exception {
        int numRequests = randomIntBetween(5, 20);
        final CountDownLatch latch = new CountDownLatch(numRequests);
        final List<TestResponse> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            final String method = RestClientTestUtil.randomHttpMethod(getRandom());
            final int statusCode = randomStatusCode(getRandom());
            restClient.performRequestAsync(method, "/" + statusCode, new ResponseListener() {
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
        for (TestResponse response : responses) {
            assertEquals(response.method, response.getResponse().getRequestLine().getMethod());
            assertEquals(response.statusCode, response.getResponse().getStatusLine().getStatusCode());

        }
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
}
