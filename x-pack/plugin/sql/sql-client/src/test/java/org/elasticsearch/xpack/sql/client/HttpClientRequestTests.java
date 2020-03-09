/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.client;

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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

public class HttpClientRequestTests extends ESTestCase {
    
    private static RawRequestMockWebServer webServer = new RawRequestMockWebServer();
    private static final Logger logger = LogManager.getLogger(HttpClientRequestTests.class);
    
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
    
    public void testBinaryRequestForCLIEnabled() throws URISyntaxException {
        assertBinaryRequestForCLI(true, XContentType.CBOR);
    }
    
    public void testBinaryRequestForCLIDisabled() throws URISyntaxException {
        assertBinaryRequestForCLI(false, XContentType.JSON);
    }
    
    public void testBinaryRequestForDriversEnabled() throws URISyntaxException {
        assertBinaryRequestForDrivers(true, XContentType.CBOR);
    }
    
    public void testBinaryRequestForDriversDisabled() throws URISyntaxException {
        assertBinaryRequestForDrivers(false, XContentType.JSON);
    }
    
    private void assertBinaryRequestForCLI(boolean isBinary, XContentType xContentType) throws URISyntaxException {
        String url = "http://" + webServer.getHostName() + ":" + webServer.getPort();
        String query = randomAlphaOfLength(256);
        int fetchSize = randomIntBetween(1, 100);
        Properties props = new Properties();
        props.setProperty(ConnectionConfiguration.BINARY_COMMUNICATION, Boolean.toString(isBinary));
        
        URI uri = new URI(url);
        ConnectionConfiguration conCfg = new ConnectionConfiguration(uri, url, props);
        HttpClient httpClient = new HttpClient(conCfg);
        
        prepareMockResponse();
        try {
            httpClient.basicQuery(query, fetchSize);
        } catch (SQLException e) {
            logger.info("Ignored SQLException", e);
        }
        assertEquals(1, webServer.requests().size());
        RawRequest recordedRequest = webServer.takeRequest();
        assertEquals(xContentType.mediaTypeWithoutParameters(), recordedRequest.getHeader("Content-Type"));
        assertEquals("POST", recordedRequest.getMethod());
        
        BytesReference bytesRef = recordedRequest.getBodyAsBytes();
        Map<String, Object> reqContent = XContentHelper.convertToMap(bytesRef, false, xContentType).v2();
        
        assertTrue(((String) reqContent.get("mode")).equalsIgnoreCase(Mode.CLI.toString()));
        assertEquals(isBinary, reqContent.get("binary_format"));
        assertEquals(Boolean.FALSE, reqContent.get("columnar"));
        assertEquals(fetchSize, reqContent.get("fetch_size"));
        assertEquals(query, reqContent.get("query"));
        assertEquals("90000ms", reqContent.get("request_timeout"));
        assertEquals("45000ms", reqContent.get("page_timeout"));
        assertEquals("Z", reqContent.get("time_zone"));
        
        prepareMockResponse();
        try {
            // we don't care what the cursor is, because the ES node that will actually handle the request (as in running an ES search)
            // will not see/have access to the "binary_format" response, which is the concern of the first node getting the request
            httpClient.nextPage("");
        } catch (SQLException e) {
            logger.info("Ignored SQLException", e);
        }
        assertEquals(1, webServer.requests().size());
        recordedRequest = webServer.takeRequest();
        assertEquals(xContentType.mediaTypeWithoutParameters(), recordedRequest.getHeader("Content-Type"));
        assertEquals("POST", recordedRequest.getMethod());
        
        bytesRef = recordedRequest.getBodyAsBytes();
        reqContent = XContentHelper.convertToMap(bytesRef, false, xContentType).v2();
        
        assertTrue(((String) reqContent.get("mode")).equalsIgnoreCase(Mode.CLI.toString()));
        assertEquals(isBinary, reqContent.get("binary_format"));
        assertEquals("90000ms", reqContent.get("request_timeout"));
        assertEquals("45000ms", reqContent.get("page_timeout"));
    }
    
    private void assertBinaryRequestForDrivers(boolean isBinary, XContentType xContentType) throws URISyntaxException {
        String url = "http://" + webServer.getHostName() + ":" + webServer.getPort();
        String query = randomAlphaOfLength(256);
        Properties props = new Properties();
        props.setProperty(ConnectionConfiguration.BINARY_COMMUNICATION, Boolean.toString(isBinary));
        
        URI uri = new URI(url);
        ConnectionConfiguration conCfg = new ConnectionConfiguration(uri, url, props);
        HttpClient httpClient = new HttpClient(conCfg);
        
        Mode mode = randomFrom(Mode.JDBC, Mode.ODBC);
        SqlQueryRequest request = new SqlQueryRequest(query,
                null,
                ZoneId.of("Z"),
                randomIntBetween(1, 100),
                TimeValue.timeValueMillis(randomNonNegativeLong()),
                TimeValue.timeValueMillis(randomNonNegativeLong()),
                null,
                randomBoolean(),
                randomAlphaOfLength(128),
                new RequestInfo(mode),
                randomBoolean(),
                randomBoolean(),
                isBinary);
        
        prepareMockResponse();
        try {
            httpClient.query(request);
        } catch (SQLException e) {
            logger.info("Ignored SQLException", e);
        }
        assertEquals(1, webServer.requests().size());
        RawRequest recordedRequest = webServer.takeRequest();
        assertEquals(xContentType.mediaTypeWithoutParameters(), recordedRequest.getHeader("Content-Type"));
        assertEquals("POST", recordedRequest.getMethod());
        
        BytesReference bytesRef = recordedRequest.getBodyAsBytes();
        Map<String, Object> reqContent = XContentHelper.convertToMap(bytesRef, false, xContentType).v2();
        
        assertTrue(((String) reqContent.get("mode")).equalsIgnoreCase(mode.toString()));
        assertEquals(isBinary, reqContent.get("binary_format"));
        assertEquals(query, reqContent.get("query"));
        assertEquals("Z", reqContent.get("time_zone"));
    }
    
    private void prepareMockResponse() {
        webServer.enqueue(new Response().setResponseCode(200).addHeader("Content-Type", "application/json").setBody("{\"rows\":[]}"));
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
