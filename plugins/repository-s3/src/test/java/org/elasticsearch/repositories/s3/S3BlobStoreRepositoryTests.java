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
package org.elasticsearch.repositories.s3;

import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.services.s3.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.nullValue;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
public class S3BlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
    }

    @Before
    public void setUpHttpServer() {
        HttpHandler handler = new InternalHttpHandler();
        if (randomBoolean()) {
            handler = new ErroneousHttpHandler(handler, randomIntBetween(2, 3));
        }
        httpServer.createContext("/bucket", handler);
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @After
    public void tearDownHttpServer() {
        httpServer.removeContext("/bucket");
    }

    @Override
    protected String repositoryType() {
        return S3Repository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            .put(S3Repository.CLIENT_NAME.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestS3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "access");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "secret");

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();

        return Settings.builder()
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            // Disable chunked encoding as it simplifies a lot the request parsing on the httpServer side
            .put(S3ClientSettings.DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace("test").getKey(), true)
            // Disable request throttling because some random values in tests might generate too many failures for the S3 client
            .put(S3ClientSettings.USE_THROTTLE_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), false)
            .put(super.nodeSettings(nodeOrdinal))
            .setSecureSettings(secureSettings)
            .build();
    }

    public static class TestS3RepositoryPlugin extends S3RepositoryPlugin {

        public TestS3RepositoryPlugin(final Settings settings) {
            super(settings);
        }

        @Override
        public List<Setting<?>> getSettings() {
            final List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(S3ClientSettings.DISABLE_CHUNKED_ENCODING);
            return settings;
        }
    }

    /**
     * Minimal HTTP handler that acts as a S3 compliant server
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class InternalHttpHandler implements HttpHandler {

        private final ConcurrentMap<String, BytesReference> blobs = new ConcurrentHashMap<>();

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            try {
                if (Regex.simpleMatch("PUT /bucket/*", request)) {
                    blobs.put(exchange.getRequestURI().toString(), Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /bucket/?prefix=*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                    assertThat("Test must be adapted for GET Bucket (List Objects) Version 2", params.get("list-type"), nullValue());

                    final StringBuilder list = new StringBuilder();
                    list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    list.append("<ListBucketResult>");
                    final String prefix = params.get("prefix");
                    if (prefix != null) {
                        list.append("<Prefix>").append(prefix).append("</Prefix>");
                    }
                    for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                        if (prefix == null || blob.getKey().startsWith("/bucket/" + prefix)) {
                            list.append("<Contents>");
                            list.append("<Key>").append(blob.getKey().replace("/bucket/", "")).append("</Key>");
                            list.append("<Size>").append(blob.getValue().length()).append("</Size>");
                            list.append("</Contents>");
                        }
                    }
                    list.append("</ListBucketResult>");

                    byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("GET /bucket/*", request)) {
                    final BytesReference blob = blobs.get(exchange.getRequestURI().toString());
                    if (blob != null) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                        blob.writeTo(exchange.getResponseBody());
                    } else {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    }

                } else if (Regex.simpleMatch("DELETE /bucket/*", request)) {
                    int deletions = 0;
                    for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = iterator.next();
                        if (blob.getKey().startsWith(exchange.getRequestURI().toString())) {
                            iterator.remove();
                            deletions++;
                        }
                    }
                    exchange.sendResponseHeaders((deletions > 0 ? RestStatus.OK : RestStatus.NO_CONTENT).getStatus(), -1);

                } else if (Regex.simpleMatch("POST /bucket/?delete", request)) {
                    final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));

                    final StringBuilder deletes = new StringBuilder();
                    deletes.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    deletes.append("<DeleteResult>");
                    for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = iterator.next();
                        String key = blob.getKey().replace("/bucket/", "");
                        if (requestBody.contains("<Key>" + key + "</Key>")) {
                            deletes.append("<Deleted><Key>").append(key).append("</Key></Deleted>");
                            iterator.remove();
                        }
                    }
                    deletes.append("</DeleteResult>");

                    byte[] response = deletes.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else {
                    exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
                }
            } finally {
                exchange.close();
            }
        }
    }

    /**
     * HTTP handler that injects random S3 service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite and/or could trigger SDK client request throttling (and request
     * would fail before reaching the max retry attempts - this can be mitigated by disabling
     * {@link S3ClientSettings#USE_THROTTLE_RETRIES_SETTING})
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class ErroneousHttpHandler implements HttpHandler {

        // first key is the remote address, second key is the HTTP request unique id provided by the AWS SDK client,
        // value is the number of times the request has been seen
        private final Map<String, AtomicInteger> requests;
        private final HttpHandler delegate;
        private final int maxErrorsPerRequest;

        private ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            this.requests = new ConcurrentHashMap<>();
            this.delegate = delegate;
            this.maxErrorsPerRequest = maxErrorsPerRequest;
            assert maxErrorsPerRequest > 1;
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String requestId = exchange.getRequestHeaders().getFirst(AmazonHttpClient.HEADER_SDK_TRANSACTION_ID);
            assert Strings.hasText(requestId);

            final int count = requests.computeIfAbsent(requestId, req -> new AtomicInteger(0)).incrementAndGet();
            if (count >= maxErrorsPerRequest || randomBoolean()) {
                requests.remove(requestId);
                delegate.handle(exchange);
            } else {
                handleAsError(exchange, requestId);
            }
        }

        private void handleAsError(final HttpExchange exchange, final String requestId) throws IOException {
            Streams.readFully(exchange.getRequestBody());
            exchange.getResponseHeaders().add(Headers.REQUEST_ID, requestId);
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            exchange.close();
        }
    }
}
