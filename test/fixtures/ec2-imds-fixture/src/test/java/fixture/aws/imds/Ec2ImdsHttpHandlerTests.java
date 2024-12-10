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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.aMapWithSize;

public class Ec2ImdsHttpHandlerTests extends ESTestCase {

    private static final String SECURITY_CREDENTIALS_URI = "/latest/meta-data/iam/security-credentials/";

    public void testImdsV1() throws IOException {
        final Map<String, String> generatedCredentials = new HashMap<>();

        final var handler = new Ec2ImdsHttpHandler(Ec2ImdsVersion.V1, generatedCredentials::put, Set.of());

        final var roleResponse = handleRequest(handler, "GET", SECURITY_CREDENTIALS_URI);
        assertEquals(RestStatus.OK, roleResponse.status());
        final var profileName = roleResponse.body().utf8ToString();
        assertTrue(Strings.hasText(profileName));

        final var credentialsResponse = handleRequest(handler, "GET", SECURITY_CREDENTIALS_URI + profileName);
        assertEquals(RestStatus.OK, credentialsResponse.status());

        assertThat(generatedCredentials, aMapWithSize(1));
        final var accessKey = generatedCredentials.keySet().iterator().next();
        final var sessionToken = generatedCredentials.values().iterator().next();

        final var responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), credentialsResponse.body().streamInput(), false);
        assertEquals(Set.of("AccessKeyId", "Expiration", "RoleArn", "SecretAccessKey", "Token"), responseMap.keySet());
        assertEquals(accessKey, responseMap.get("AccessKeyId"));
        assertEquals(sessionToken, responseMap.get("Token"));
    }

    public void testImdsV2Disabled() {
        assertEquals(
            RestStatus.METHOD_NOT_ALLOWED,
            handleRequest(
                new Ec2ImdsHttpHandler(Ec2ImdsVersion.V1, (accessKey, sessionToken) -> fail(), Set.of()),
                "PUT",
                "/latest/api/token"
            ).status()
        );
    }

    public void testImdsV2() throws IOException {
        final Map<String, String> generatedCredentials = new HashMap<>();

        final var handler = new Ec2ImdsHttpHandler(Ec2ImdsVersion.V2, generatedCredentials::put, Set.of());

        final var tokenResponse = handleRequest(handler, "PUT", "/latest/api/token");
        assertEquals(RestStatus.OK, tokenResponse.status());
        final var token = tokenResponse.body().utf8ToString();

        final var roleResponse = checkImdsV2GetRequest(handler, SECURITY_CREDENTIALS_URI, token);
        assertEquals(RestStatus.OK, roleResponse.status());
        final var profileName = roleResponse.body().utf8ToString();
        assertTrue(Strings.hasText(profileName));

        final var credentialsResponse = checkImdsV2GetRequest(handler, SECURITY_CREDENTIALS_URI + profileName, token);
        assertEquals(RestStatus.OK, credentialsResponse.status());

        assertThat(generatedCredentials, aMapWithSize(1));
        final var accessKey = generatedCredentials.keySet().iterator().next();
        final var sessionToken = generatedCredentials.values().iterator().next();

        final var responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), credentialsResponse.body().streamInput(), false);
        assertEquals(Set.of("AccessKeyId", "Expiration", "RoleArn", "SecretAccessKey", "Token"), responseMap.keySet());
        assertEquals(accessKey, responseMap.get("AccessKeyId"));
        assertEquals(sessionToken, responseMap.get("Token"));
    }

    private record TestHttpResponse(RestStatus status, BytesReference body) {}

    private static TestHttpResponse checkImdsV2GetRequest(Ec2ImdsHttpHandler handler, String uri, String token) {
        final var unauthorizedResponse = handleRequest(handler, "GET", uri, null);
        assertEquals(RestStatus.UNAUTHORIZED, unauthorizedResponse.status());

        final var forbiddenResponse = handleRequest(handler, "GET", uri, randomValueOtherThan(token, ESTestCase::randomSecretKey));
        assertEquals(RestStatus.FORBIDDEN, forbiddenResponse.status());

        return handleRequest(handler, "GET", uri, token);
    }

    private static TestHttpResponse handleRequest(Ec2ImdsHttpHandler handler, String method, String uri) {
        return handleRequest(handler, method, uri, null);
    }

    private static TestHttpResponse handleRequest(Ec2ImdsHttpHandler handler, String method, String uri, @Nullable String token) {
        final Headers headers;
        if (token == null) {
            headers = TestHttpExchange.EMPTY_HEADERS;
        } else {
            headers = new Headers();
            headers.put("X-aws-ec2-metadata-token", List.of(token));
        }

        final var httpExchange = new TestHttpExchange(method, uri, BytesArray.EMPTY, headers);
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
