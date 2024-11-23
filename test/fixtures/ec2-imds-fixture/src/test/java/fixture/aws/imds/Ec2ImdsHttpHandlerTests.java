/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws.imds;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Set;

public class Ec2ImdsHttpHandlerTests extends ESTestCase {

    public void testImdsV1() throws IOException {
        final var accessKey = randomIdentifier();
        final var secretKey = randomIdentifier();
        final var sessionToken = randomIdentifier();

        final var handler = new Ec2ImdsHttpHandler(accessKey, secretKey, sessionToken, Set.of());

        final var roleResponse = handleRequest(handler, "GET", "/latest/meta-data/iam/security-credentials/");
        assertEquals(RestStatus.OK, roleResponse.status());
        assertEquals(new BytesArray("ec2Profile"), roleResponse.body());

        final var credentialsResponse = handleRequest(handler, "GET", "/latest/meta-data/iam/security-credentials/ec2Profile");
        assertEquals(RestStatus.OK, credentialsResponse.status());

        final var responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), credentialsResponse.body().streamInput(), false);
        assertEquals(Set.of("AccessKeyId", "Expiration", "RoleArn", "SecretAccessKey", "Token"), responseMap.keySet());
        assertEquals(accessKey, responseMap.get("AccessKeyId"));
        assertEquals(secretKey, responseMap.get("SecretAccessKey"));
        assertEquals(sessionToken, responseMap.get("Token"));
    }

    public void testImdsV2Disabled() {
        assertEquals(
            RestStatus.METHOD_NOT_ALLOWED,
            handleRequest(
                new Ec2ImdsHttpHandler(randomIdentifier(), randomIdentifier(), randomIdentifier(), Set.of()),
                "PUT",
                "/latest/api/token"
            ).status()
        );
    }

    private record TestHttpResponse(RestStatus status, BytesReference body) {}

    private static TestHttpResponse handleRequest(Ec2ImdsHttpHandler handler, String method, String uri) {
        final var httpExchange = new TestHttpExchange(method, uri, BytesArray.EMPTY, TestHttpExchange.EMPTY_HEADERS);
        try {
            handler.handle(httpExchange);
        } catch (IOException e) {
            fail(e);
        }
        assertNotEquals(0, httpExchange.getResponseCode());
        return new TestHttpResponse(RestStatus.fromCode(httpExchange.getResponseCode()), httpExchange.getResponseBodyContents());
    }

    private static class TestHttpExchange extends HttpExchange {

        private static final Headers EMPTY_HEADERS = new Headers();

        private final String method;
        private final URI uri;
        private final BytesReference requestBody;
        private final Headers requestHeaders;

        private final Headers responseHeaders = new Headers();
        private final BytesStreamOutput responseBody = new BytesStreamOutput();
        private int responseCode;

        TestHttpExchange(String method, String uri, BytesReference requestBody, Headers requestHeaders) {
            this.method = method;
            this.uri = URI.create(uri);
            this.requestBody = requestBody;
            this.requestHeaders = requestHeaders;
        }

        @Override
        public Headers getRequestHeaders() {
            return requestHeaders;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public URI getRequestURI() {
            return uri;
        }

        @Override
        public String getRequestMethod() {
            return method;
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            try {
                return requestBody.streamInput();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {
            this.responseCode = rCode;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public int getResponseCode() {
            return responseCode;
        }

        public BytesReference getResponseBodyContents() {
            return responseBody.bytes();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
            fail("setAttribute not implemented");
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            fail("setStreams not implemented");
        }

        @Override
        public HttpPrincipal getPrincipal() {
            fail("getPrincipal not implemented");
            throw new UnsupportedOperationException("getPrincipal not implemented");
        }
    }

}
