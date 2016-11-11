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

import static org.elasticsearch.client.RestClientTestUtil.getAllStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.getHttpMethods;
import static org.elasticsearch.client.RestClientTestUtil.randomStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test to check interaction between {@link RestClient} and {@link org.apache.http.client.HttpClient}.
 * Works against a real http server, one single host.
 */
//animal-sniffer doesn't like our usage of com.sun.net.httpserver.* classes
@IgnoreJRERequirement
public class RestClientSingleHostIntegTests extends RestClientTestCase {

    private static HttpServer httpServer;
    private static RestClient restClient;
    private static String pathPrefix;
    private static Header[] defaultHeaders;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        String pathPrefixWithoutLeadingSlash;
        if (randomBoolean()) {
            pathPrefixWithoutLeadingSlash = "testPathPrefix/" + randomAsciiOfLengthBetween(1, 5);
            pathPrefix = "/" + pathPrefixWithoutLeadingSlash;
        } else {
            pathPrefix = pathPrefixWithoutLeadingSlash = "";
        }

        httpServer = createHttpServer();
        int numHeaders = randomIntBetween(0, 5);
        defaultHeaders = generateHeaders("Header-default", "Header-array", numHeaders);
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort())).setDefaultHeaders(defaultHeaders);
        if (pathPrefix.length() > 0) {
            restClientBuilder.setPathPrefix((randomBoolean() ? "/" : "") + pathPrefixWithoutLeadingSlash);
        }
        restClient = restClientBuilder.build();
    }

    private static HttpServer createHttpServer() throws Exception {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        //returns a different status code depending on the path
        for (int statusCode : getAllStatusCodes()) {
            httpServer.createContext(pathPrefix + "/" + statusCode, new ResponseHandler(statusCode));
        }
        return httpServer;
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

            assertEquals(method, esResponse.getRequestLine().getMethod());
            assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
            assertEquals((pathPrefix.length() > 0 ? pathPrefix : "") + "/" + statusCode, esResponse.getRequestLine().getUri());

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
        assertEquals(method, esResponse.getRequestLine().getMethod());
        assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
        assertEquals((pathPrefix.length() > 0 ? pathPrefix : "") + "/" + statusCode, esResponse.getRequestLine().getUri());
        assertEquals(requestBody, EntityUtils.toString(esResponse.getEntity()));
    }
}
