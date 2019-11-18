/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.ConnectionConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

public class JdbcHttpClientRequestTests extends ESTestCase {
    
    private static RawRequestMockWebServer webServer = new RawRequestMockWebServer();
    private static final Logger logger = LogManager.getLogger(JdbcHttpClientRequestTests.class);
    
    @BeforeClass
    public static void init() throws Exception {
        webServer.start();
    }

    @AfterClass
    public static void cleanup() {
        try {
            webServer.close();
        } finally {
            webServer = null;
        }
    }

    public void testBinaryRequestEnabled() throws Exception {
        assertBinaryRequest(true, XContentType.CBOR);
    }
    
    public void testBinaryRequestDisabled() throws Exception {
        assertBinaryRequest(false, XContentType.JSON);
    }

    private void assertBinaryRequest(boolean isBinary, XContentType xContentType) throws Exception {
        String url = JdbcConfiguration.URL_PREFIX + webServer.getHostName() + ":" + webServer.getPort();
        Properties props = new Properties();
        props.setProperty(ConnectionConfiguration.BINARY_COMMUNICATION, Boolean.toString(isBinary));
        
        JdbcHttpClient httpClient = new JdbcHttpClient(JdbcConfiguration.create(url, props, 0), false);
        
        prepareMockResponse();
        try {
            httpClient.query(randomAlphaOfLength(256), null,
                             new RequestMeta(randomIntBetween(1, 100), randomNonNegativeLong(), randomNonNegativeLong()));
        } catch (SQLException e) {
            logger.info("Ignored SQLException", e);
        }
        assertValues(isBinary, xContentType);
        
        prepareMockResponse();
        try {
            httpClient.nextPage("", new RequestMeta(randomIntBetween(1, 100), randomNonNegativeLong(), randomNonNegativeLong()));
        } catch (SQLException e) {
            logger.info("Ignored SQLException", e);
        }
        assertValues(isBinary, xContentType);
    }

    private void assertValues(boolean isBinary, XContentType xContentType) {
        assertEquals(1, webServer.requests().size());
        RawRequest recordedRequest = webServer.takeRequest();
        assertEquals(xContentType.mediaTypeWithoutParameters(), recordedRequest.getHeader("Content-Type"));
        assertEquals("POST", recordedRequest.getMethod());
        
        BytesReference bytesRef = recordedRequest.getBodyAsBytes();
        Map<String, Object> reqContent = XContentHelper.convertToMap(bytesRef, false, xContentType).v2();
        
        assertTrue(((String) reqContent.get("mode")).equalsIgnoreCase("jdbc"));
        assertEquals(isBinary, reqContent.get("binary_format"));
    }
    
    private void prepareMockResponse() {
        webServer.enqueue(new Response()
                          .setResponseCode(200)
                          .addHeader("Content-Type", "application/json")
                          .setBody("{\"rows\":[],\"columns\":[]}"));
    }
    
    @SuppressForbidden(reason = "use http server")
    private static class RawRequestMockWebServer implements Closeable {
        private HttpServer server;
        private final Queue<Response> responses = ConcurrentCollections.newQueue();
        private final Queue<RawRequest> requests = ConcurrentCollections.newQueue();
        private String hostname;
        private int port;

        RawRequestMockWebServer() {
        }

        void start() throws IOException {
            InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
            server = MockHttpServer.createHttp(address, 0);

            server.start();
            this.hostname = server.getAddress().getHostString();
            this.port = server.getAddress().getPort();
            
            server.createContext("/", s -> {
                try {
                    Response response = responses.poll();
                    RawRequest request = createRequest(s);
                    requests.add(request);
                    s.getResponseHeaders().putAll(response.getHeaders());

                    if (Strings.isEmpty(response.getBody())) {
                        s.sendResponseHeaders(response.getStatusCode(), 0);
                    } else {
                        byte[] responseAsBytes = response.getBody().getBytes(StandardCharsets.UTF_8);
                        s.sendResponseHeaders(response.getStatusCode(), responseAsBytes.length);
                        if ("HEAD".equals(request.getMethod()) == false) {
                            try (OutputStream responseBody = s.getResponseBody()) {
                                responseBody.write(responseAsBytes);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to respond to request [{} {}]",
                            s.getRequestMethod(), s.getRequestURI()), e);
                } finally {
                    s.close();
                }

            });
        }

        private RawRequest createRequest(HttpExchange exchange) throws IOException {
            RawRequest request = new RawRequest(exchange.getRequestMethod(), exchange.getRequestHeaders());
            if (exchange.getRequestBody() != null) {
                BytesReference bytesRef = Streams.readFully(exchange.getRequestBody());
                request.setBodyAsBytes(bytesRef);
            }
            return request;
        }

        String getHostName() {
            return hostname;
        }

        int getPort() {
            return port;
        }

        void enqueue(Response response) {
            responses.add(response);
        }

        List<RawRequest> requests() {
            return new ArrayList<>(requests);
        }

        RawRequest takeRequest() {
            return requests.poll();
        }

        @Override
        public void close() {
            if (server.getExecutor() instanceof ExecutorService) {
                terminate((ExecutorService) server.getExecutor());
            }
            server.stop(0);
        }
    }

    @SuppressForbidden(reason = "use http server header class")
    private static class RawRequest {
        
        private final String method;
        private final Headers headers;
        private BytesReference bodyAsBytes = null;

        RawRequest(String method, Headers headers) {
            this.method = method;
            this.headers = headers;
        }

        public String getMethod() {
            return method;
        }

        public String getHeader(String name) {
            return headers.getFirst(name);
        }

        public BytesReference getBodyAsBytes() {
            return bodyAsBytes;
        }

        public void setBodyAsBytes(BytesReference bodyAsBytes) {
            this.bodyAsBytes = bodyAsBytes;
        }
    }
    
    @SuppressForbidden(reason = "use http server header class")
    private class Response {

        private String body = null;
        private int statusCode = 200;
        private Headers headers = new Headers();

        public Response setBody(String body) {
            this.body = body;
            return this;
        }

        public Response setResponseCode(int statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Response addHeader(String name, String value) {
            headers.add(name, value);
            return this;
        }

        String getBody() {
            return body;
        }

        int getStatusCode() {
            return statusCode;
        }

        Headers getHeaders() {
            return headers;
        }
    }
}
