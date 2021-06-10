/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.blobstore.url.http.URLHttpClient.createErrorMessage;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

@SuppressForbidden(reason = "uses a http server")
public class URLHttpClientTests extends ESTestCase {
    private static HttpServer httpServer;
    private static URLHttpClient.Factory httpClientFactory;
    private static URLHttpClient httpClient;

    @BeforeClass
    public static void setUpHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        httpClientFactory = new URLHttpClient.Factory();
        final Settings settings = Settings.builder()
            .put("http_max_retries", 0)
            .build();
        httpClient = httpClientFactory.create(URLHttpClientSettings.fromSettings(settings));
    }

    @AfterClass
    public static void tearDownHttpServer() throws Exception {
        httpServer.stop(1);
        httpClient.close();
        httpClientFactory.close();
    }

    public void testSuccessfulRequest() throws Exception {
        byte[] originalData = randomByteArrayOfLength(randomIntBetween(100, 1024));
        RestStatus statusCode =
            randomFrom(RestStatus.OK, RestStatus.PARTIAL_CONTENT);

        httpServer.createContext("/correct_data", exchange -> {
            try {
                assertThat(exchange.getRequestMethod(), equalTo("GET"));
                Streams.readFully(exchange.getRequestBody());

                exchange.sendResponseHeaders(statusCode.getStatus(), originalData.length);
                exchange.getResponseBody().write(originalData);
            } finally {
                exchange.close();
            }
        });

        final URLHttpClient.HttpResponse httpResponse = executeRequest("/correct_data");
        final BytesReference responseBytes = Streams.readFully(httpResponse.getInputStream());

        assertThat(httpResponse.getStatusCode(), equalTo(statusCode.getStatus()));
        assertThat(BytesReference.toBytes(responseBytes), equalTo(originalData));
    }

    public void testEmptyErrorMessageBody() {
        final Integer errorCode = randomFrom(RestStatus.BAD_GATEWAY.getStatus(),
            RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus(), RestStatus.INTERNAL_SERVER_ERROR.getStatus());

        httpServer.createContext("/empty_error", exchange -> {
            assertThat(exchange.getRequestMethod(), equalTo("GET"));
            Streams.readFully(exchange.getRequestBody());

            try {
                final Headers responseHeaders = exchange.getResponseHeaders();
                if (randomBoolean()) {
                    // Invalid content-type
                    final String contentType = randomAlphaOfLength(100);
                    Charset charset = Charset.forName("ISO-8859-4");
                    String errorMessage = randomAlphaOfLength(100);
                    responseHeaders.add("Content-Type", contentType + "; charset=" + charset.name());
                    final byte[] errorMessageBytes = errorMessage.getBytes(charset);
                    exchange.sendResponseHeaders(errorCode, errorMessageBytes.length);
                    exchange.getResponseBody().write(errorMessageBytes);
                } else {
                    // Empty body
                    exchange.sendResponseHeaders(errorCode, -1);
                }
            } finally {
                exchange.close();
            }
        });

        final URLHttpClientException urlHttpClientException =
            expectThrows(URLHttpClientException.class, () -> executeRequest("/empty_error"));

        assertThat(urlHttpClientException.getMessage(), is(createErrorMessage(errorCode, "")));
        assertThat(urlHttpClientException.getStatusCode(), equalTo(errorCode));
    }

    public void testErrorMessageParsing() {
        final Charset charset;
        final String errorMessage;
        final int errorMessageSize = randomIntBetween(1, 100);
        if (randomBoolean()) {
            charset = Charset.forName("ISO-8859-4");
            errorMessage = randomAlphaOfLength(errorMessageSize);
        } else {
            charset = StandardCharsets.UTF_8;
            errorMessage = randomUnicodeOfLength(errorMessageSize);
        }
        final Integer errorCode = randomFrom(RestStatus.BAD_GATEWAY.getStatus(),
            RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus(), RestStatus.INTERNAL_SERVER_ERROR.getStatus());

        httpServer.createContext("/error", exchange -> {
            assertThat(exchange.getRequestMethod(), equalTo("GET"));
            Streams.readFully(exchange.getRequestBody());

            try {
                final Headers responseHeaders = exchange.getResponseHeaders();
                final String contentType = randomFrom("text/plain", "text/html", "application/json", "application/xml");
                responseHeaders.add("Content-Type", contentType + "; charset=" + charset.name());
                final byte[] errorMessageBytes = errorMessage.getBytes(charset);
                exchange.sendResponseHeaders(errorCode, errorMessageBytes.length);
                exchange.getResponseBody().write(errorMessageBytes);
            } finally {
                exchange.close();
            }
        });

        final URLHttpClientException urlHttpClientException =
            expectThrows(URLHttpClientException.class, () -> executeRequest("/error"));

        assertThat(urlHttpClientException.getMessage(), equalTo(createErrorMessage(errorCode, errorMessage)));
        assertThat(urlHttpClientException.getStatusCode(), equalTo(errorCode));
    }

    public void testLargeErrorMessageIsBounded() throws Exception {
        final Charset charset;
        final String errorMessage;
        final int errorMessageSize = randomIntBetween(URLHttpClient.MAX_ERROR_MESSAGE_BODY_SIZE + 1,
            URLHttpClient.MAX_ERROR_MESSAGE_BODY_SIZE * 2);
        if (randomBoolean()) {
            charset = Charset.forName("ISO-8859-4");
            errorMessage = randomAlphaOfLength(errorMessageSize);
        } else {
            charset = StandardCharsets.UTF_8;
            errorMessage = randomUnicodeOfCodepointLength(errorMessageSize);
        }
        final Integer errorCode = randomFrom(RestStatus.BAD_GATEWAY.getStatus(),
            RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus(), RestStatus.INTERNAL_SERVER_ERROR.getStatus());

        httpServer.createContext("/large_error", exchange -> {
            assertThat(exchange.getRequestMethod(), equalTo("GET"));
            Streams.readFully(exchange.getRequestBody());

            try {
                final Headers responseHeaders = exchange.getResponseHeaders();
                final String contentType = randomFrom("text/plain", "text/html", "application/json", "application/xml");
                responseHeaders.add("Content-Type", contentType + "; charset=" + charset.name());

                final byte[] errorMessageBytes = errorMessage.getBytes(charset);
                exchange.sendResponseHeaders(errorCode, errorMessageBytes.length);

                exchange.getResponseBody().write(errorMessageBytes);
            } finally {
                exchange.close();
            }
        });

        final URLHttpClientException urlHttpClientException =
            expectThrows(URLHttpClientException.class, () -> executeRequest("/large_error"));

        final byte[] bytes = errorMessage.getBytes(charset);
        final String strippedErrorMessage = new String(Arrays.copyOf(bytes, URLHttpClient.MAX_ERROR_MESSAGE_BODY_SIZE), charset);

        assertThat(urlHttpClientException.getMessage(), equalTo(createErrorMessage(errorCode, strippedErrorMessage)));
        assertThat(urlHttpClientException.getStatusCode(), equalTo(errorCode));
    }

    public void testInvalidErrorMessageCharsetIsIgnored() {
        final Integer errorCode = randomFrom(RestStatus.BAD_GATEWAY.getStatus(),
            RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus(), RestStatus.INTERNAL_SERVER_ERROR.getStatus());

        httpServer.createContext("/unknown_charset", exchange -> {
            assertThat(exchange.getRequestMethod(), equalTo("GET"));
            Streams.readFully(exchange.getRequestBody());

            try {
                final Headers responseHeaders = exchange.getResponseHeaders();
                final String contentType = randomFrom("text/plain", "text/html", "application/json", "application/xml");
                responseHeaders.add("Content-Type", contentType + "; charset=" + randomAlphaOfLength(4));

                final byte[] errorMessageBytes = randomByteArrayOfLength(randomIntBetween(1, 100));
                exchange.sendResponseHeaders(errorCode, errorMessageBytes.length);

                exchange.getResponseBody().write(errorMessageBytes);
            } finally {
                exchange.close();
            }
        });

        final URLHttpClientException urlHttpClientException =
            expectThrows(URLHttpClientException.class, () -> executeRequest("/unknown_charset"));

        assertThat(urlHttpClientException.getMessage(), is(createErrorMessage(errorCode, "")));
        assertThat(urlHttpClientException.getStatusCode(), equalTo(errorCode));
    }

    private URLHttpClient.HttpResponse executeRequest(String endpoint) throws Exception {
        return AccessController.doPrivileged((PrivilegedExceptionAction<URLHttpClient.HttpResponse>) () -> {
            return httpClient.get(getURIForEndpoint(endpoint), Map.of());
        });
    }

    private URI getURIForEndpoint(String endpoint) throws Exception {
        InetSocketAddress address = httpServer.getAddress();
        return new URI("http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + endpoint);
    }
}
