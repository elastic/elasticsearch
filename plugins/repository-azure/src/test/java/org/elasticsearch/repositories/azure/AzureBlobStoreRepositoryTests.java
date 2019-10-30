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
package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/container", new InternalHttpHandler());
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new AzureErroneousHttpHandler(delegate, randomIntBetween(2, 3));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), "account");
        secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);

        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * AzureRepositoryPlugin that allows to set low values for the Azure's client retry policy
     * and for BlobRequestOptions#getSingleBlobPutThresholdInBytes().
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        AzureStorageService createAzureStoreService(final Settings settings) {
            return new AzureStorageService(settings) {
                @Override
                RetryPolicyFactory createRetryPolicy(final AzureStorageSettings azureStorageSettings) {
                    return new RetryExponentialRetry(1, 100, 500, azureStorageSettings.getMaxRetries());
                }

                @Override
                BlobRequestOptions getBlobRequestOptionsForWriteBlob() {
                    BlobRequestOptions options = new BlobRequestOptions();
                    options.setSingleBlobPutThresholdInBytes(Math.toIntExact(ByteSizeUnit.MB.toBytes(1)));
                    return options;
                }
            };
        }
    }

    /**
     * Minimal HTTP handler that acts as an Azure compliant server
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class InternalHttpHandler implements HttpHandler {

        private final Map<String, BytesReference> blobs = new ConcurrentHashMap<>();

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            try {
                if (Regex.simpleMatch("PUT /container/*blockid=*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                    final String blockId = params.get("blockid");
                    blobs.put(blockId, Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

                } else if (Regex.simpleMatch("PUT /container/*comp=blocklist*", request)) {
                    final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                    final List<String> blockIds = Arrays.stream(blockList.split("<Latest>"))
                        .filter(line -> line.contains("</Latest>"))
                        .map(line -> line.substring(0, line.indexOf("</Latest>")))
                        .collect(Collectors.toList());

                    final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                    for (String blockId : blockIds) {
                        BytesReference block = blobs.remove(blockId);
                        assert block != null;
                        block.writeTo(blob);
                    }
                    blobs.put(exchange.getRequestURI().getPath(), new BytesArray(blob.toByteArray()));
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

                } else if (Regex.simpleMatch("PUT /container/*", request)) {
                    blobs.put(exchange.getRequestURI().getPath(), Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

                } else if (Regex.simpleMatch("HEAD /container/*", request)) {
                    final BytesReference blob = blobs.get(exchange.getRequestURI().getPath());
                    if (blob == null) {
                        TestUtils.sendError(exchange, RestStatus.NOT_FOUND);
                        return;
                    }
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blob.length()));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /container/*", request)) {
                    final BytesReference blob = blobs.get(exchange.getRequestURI().getPath());
                    if (blob == null) {
                        TestUtils.sendError(exchange, RestStatus.NOT_FOUND);
                        return;
                    }

                    final String range = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.STORAGE_RANGE_HEADER);
                    final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(range);
                    assertTrue(matcher.matches());

                    final int start = Integer.parseInt(matcher.group(1));
                    final int length = Integer.parseInt(matcher.group(2)) - start + 1;

                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                    exchange.getResponseBody().write(blob.toBytesRef().bytes, start, length);

                } else if (Regex.simpleMatch("DELETE /container/*", request)) {
                    drainInputStream(exchange.getRequestBody());
                    blobs.entrySet().removeIf(blob -> blob.getKey().startsWith(exchange.getRequestURI().getPath()));
                    exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /container?restype=container&comp=list*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                    final StringBuilder list = new StringBuilder();
                    list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    list.append("<EnumerationResults>");
                    final String prefix = params.get("prefix");
                    list.append("<Blobs>");
                    for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                        if (prefix == null || blob.getKey().startsWith("/container/" + prefix)) {
                            list.append("<Blob><Name>").append(blob.getKey().replace("/container/", "")).append("</Name>");
                            list.append("<Properties><Content-Length>").append(blob.getValue().length()).append("</Content-Length>");
                            list.append("<BlobType>BlockBlob</BlobType></Properties></Blob>");
                        }
                    }
                    list.append("</Blobs>");
                    list.append("</EnumerationResults>");

                    byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else {
                    TestUtils.sendError(exchange, RestStatus.BAD_REQUEST);
                }
            } finally {
                exchange.close();
            }
        }
    }

    /**
     * HTTP handler that injects random Azure service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureErroneousHttpHandler extends ErroneousHttpHandler {

        AzureErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected void handleAsError(final HttpExchange exchange) throws IOException {
            drainInputStream(exchange.getRequestBody());
            TestUtils.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            exchange.close();
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            final String requestId = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER);
            final String range = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.STORAGE_RANGE_HEADER);
            return exchange.getRequestMethod()
                + " " + requestId
                + (range != null ? " " + range : "");
        }
    }
}
