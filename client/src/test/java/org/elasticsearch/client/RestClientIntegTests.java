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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.client.RestClientTestUtil.getAllStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.getHttpMethods;
import static org.elasticsearch.client.RestClientTestUtil.randomStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Integration test to check interaction between {@link RestClient} and {@link org.apache.http.client.HttpClient}.
 * Works against a real http server, one single host.
 */
public class RestClientIntegTests extends LuceneTestCase {

    private static HttpServer httpServer;
    private static RestClient restClient;
    private static Header[] defaultHeaders;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.start();
        //returns a different status code depending on the path
        for (int statusCode : getAllStatusCodes()) {
            createStatusCodeContext(httpServer, statusCode);
        }
        int numHeaders = RandomInts.randomIntBetween(random(), 0, 3);
        defaultHeaders = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            String headerName = "Header-default" + (random().nextBoolean() ? i : "");
            String headerValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
            defaultHeaders[i] = new BasicHeader(headerName, headerValue);
        }
        restClient = RestClient.builder().setDefaultHeaders(defaultHeaders)
                .setHosts(new HttpHost(httpServer.getAddress().getHostName(), httpServer.getAddress().getPort())).build();
    }

    private static void createStatusCodeContext(HttpServer httpServer, final int statusCode) {
        httpServer.createContext("/" + statusCode, new HttpHandler() {
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
        });
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
    public void testHeaders() throws Exception {
        for (String method : getHttpMethods()) {
            Set<String> standardHeaders = new HashSet<>(
                    Arrays.asList("Accept-encoding", "Connection", "Host", "User-agent", "Date"));
            if (method.equals("HEAD") == false) {
                standardHeaders.add("Content-length");
            }
            int numHeaders = RandomInts.randomIntBetween(random(), 1, 5);
            Map<String, String> expectedHeaders = new HashMap<>();
            for (Header defaultHeader : defaultHeaders) {
                expectedHeaders.put(defaultHeader.getName(), defaultHeader.getValue());
            }
            Header[] headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                String headerName = "Header" + (random().nextBoolean() ? i : "");
                String headerValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
                headers[i] = new BasicHeader(headerName, headerValue);
                expectedHeaders.put(headerName, headerValue);
            }

            int statusCode = randomStatusCode(random());
            ElasticsearchResponse esResponse;
            try (ElasticsearchResponse response = restClient.performRequest(method, "/" + statusCode,
                    Collections.<String, String>emptyMap(), null, headers)) {
                esResponse = response;
            } catch(ElasticsearchResponseException e) {
                esResponse = e.getElasticsearchResponse();
            }
            assertThat(esResponse.getStatusLine().getStatusCode(), equalTo(statusCode));
            for (Header responseHeader : esResponse.getHeaders()) {
                if (responseHeader.getName().startsWith("Header")) {
                    String headerValue = expectedHeaders.remove(responseHeader.getName());
                    assertNotNull("found response header [" + responseHeader.getName() + "] that wasn't originally sent", headerValue);
                } else {
                    assertTrue("unknown header was returned " + responseHeader.getName(),
                            standardHeaders.remove(responseHeader.getName()));
                }
            }
            assertEquals("some headers that were sent weren't returned: " + expectedHeaders, 0, expectedHeaders.size());
            assertEquals("some expected standard headers weren't returned: " + standardHeaders, 0, standardHeaders.size());
        }
    }

    /**
     * End to end test for delete with body. We test it explicitly as it is not supported
     * out of the box by {@link org.apache.http.client.HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testDeleteWithBody() throws Exception {
        testBody("DELETE");
    }

    /**
     * End to end test for get with body. We test it explicitly as it is not supported
     * out of the box by {@link org.apache.http.client.HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testGetWithBody() throws Exception {
        testBody("GET");
    }

    private void testBody(String method) throws Exception {
        String requestBody = "{ \"field\": \"value\" }";
        StringEntity entity = new StringEntity(requestBody);
        ElasticsearchResponse esResponse;
        String responseBody;
        int statusCode = randomStatusCode(random());
        try (ElasticsearchResponse response = restClient.performRequest(method, "/" + statusCode,
                Collections.<String, String>emptyMap(), entity)) {
            responseBody = EntityUtils.toString(response.getEntity());
            esResponse = response;
        } catch(ElasticsearchResponseException e) {
            responseBody = e.getResponseBody();
            esResponse = e.getElasticsearchResponse();
        }
        assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
        assertEquals(requestBody, responseBody);
    }
}
