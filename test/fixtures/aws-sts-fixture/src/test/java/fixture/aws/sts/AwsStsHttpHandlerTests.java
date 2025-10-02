/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws.sts;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;

public class AwsStsHttpHandlerTests extends ESTestCase {

    public void testGenerateCredentials() {
        final Map<String, String> generatedCredentials = new HashMap<>();

        final var webIdentityToken = randomUnicodeOfLength(10);
        final var handler = new AwsStsHttpHandler(generatedCredentials::put, webIdentityToken);

        final var response = handleRequest(
            handler,
            Map.of(
                "Action",
                "AssumeRoleWithWebIdentity",
                "RoleSessionName",
                AwsStsHttpHandler.ROLE_NAME,
                "RoleArn",
                AwsStsHttpHandler.ROLE_ARN,
                "WebIdentityToken",
                webIdentityToken
            )
        );
        assertEquals(RestStatus.OK, response.status());

        assertThat(generatedCredentials, aMapWithSize(1));
        final var accessKey = generatedCredentials.keySet().iterator().next();
        final var sessionToken = generatedCredentials.values().iterator().next();

        final var responseBody = response.body().utf8ToString();
        assertThat(responseBody, containsString("<AccessKeyId>" + accessKey + "</AccessKeyId>"));
        assertThat(responseBody, containsString("<SessionToken>" + sessionToken + "</SessionToken>"));
    }

    public void testInvalidAction() {
        final var handler = new AwsStsHttpHandler((key, token) -> fail(), randomUnicodeOfLength(10));
        final var response = handleRequest(handler, Map.of("Action", "Unsupported"));
        assertEquals(RestStatus.BAD_REQUEST, response.status());
    }

    public void testInvalidRole() {
        final var webIdentityToken = randomUnicodeOfLength(10);
        final var handler = new AwsStsHttpHandler((key, token) -> fail(), webIdentityToken);
        final var response = handleRequest(
            handler,
            Map.of(
                "Action",
                "AssumeRoleWithWebIdentity",
                "RoleSessionName",
                randomValueOtherThan(AwsStsHttpHandler.ROLE_NAME, ESTestCase::randomIdentifier),
                "RoleArn",
                AwsStsHttpHandler.ROLE_ARN,
                "WebIdentityToken",
                webIdentityToken
            )
        );
        assertEquals(RestStatus.UNAUTHORIZED, response.status());
    }

    public void testInvalidToken() {
        final var webIdentityToken = randomUnicodeOfLength(10);
        final var handler = new AwsStsHttpHandler((key, token) -> fail(), webIdentityToken);
        final var response = handleRequest(
            handler,
            Map.of(
                "Action",
                "AssumeRoleWithWebIdentity",
                "RoleSessionName",
                AwsStsHttpHandler.ROLE_NAME,
                "RoleArn",
                AwsStsHttpHandler.ROLE_ARN,
                "WebIdentityToken",
                randomValueOtherThan(webIdentityToken, () -> randomUnicodeOfLength(10))
            )
        );
        assertEquals(RestStatus.UNAUTHORIZED, response.status());
    }

    public void testInvalidARN() {
        final var webIdentityToken = randomUnicodeOfLength(10);
        final var handler = new AwsStsHttpHandler((key, token) -> fail(), webIdentityToken);
        final var response = handleRequest(
            handler,
            Map.of(
                "Action",
                "AssumeRoleWithWebIdentity",
                "RoleSessionName",
                AwsStsHttpHandler.ROLE_NAME,
                "RoleArn",
                randomValueOtherThan(AwsStsHttpHandler.ROLE_ARN, ESTestCase::randomIdentifier),
                "WebIdentityToken",
                webIdentityToken
            )
        );
        assertEquals(RestStatus.UNAUTHORIZED, response.status());
    }

    private record TestHttpResponse(RestStatus status, BytesReference body) {}

    private static TestHttpResponse handleRequest(AwsStsHttpHandler handler, Map<String, String> body) {
        final var httpExchange = new TestHttpExchange(
            "POST",
            "/",
            new BytesArray(
                body.entrySet()
                    .stream()
                    .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                    .collect(Collectors.joining("&"))
            ),
            TestHttpExchange.EMPTY_HEADERS
        );
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
