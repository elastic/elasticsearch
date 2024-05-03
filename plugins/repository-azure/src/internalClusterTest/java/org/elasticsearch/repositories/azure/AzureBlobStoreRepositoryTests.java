/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    private static final String DEFAULT_ACCOUNT_NAME = "account";

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.repositorySettings(repoName))
            .put(AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.MB))
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test");
        if (randomBoolean()) {
            settingsBuilder.put(AzureRepository.Repository.BASE_PATH_SETTING.getKey(), randomFrom("test", "test/1"));
        }
        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap(
            "/" + DEFAULT_ACCOUNT_NAME,
            new AzureHTTPStatsCollectorHandler(new AzureBlobStoreHttpHandler(DEFAULT_ACCOUNT_NAME, "container"))
        );
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new AzureErroneousHttpHandler(delegate, AzureStorageSettings.DEFAULT_MAX_RETRIES);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        String accountName = DEFAULT_ACCOUNT_NAME;
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), accountName);
        if (randomBoolean()) {
            secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);
        } else {
            // The SDK expects a valid SAS TOKEN
            secureSettings.setString(
                AzureStorageSettings.SAS_TOKEN_SETTING.getConcreteSettingForNamespace("test").getKey(),
                "se=2021-07-20T13%3A21Z&sp=rwdl&sv=2018-11-09&sr=c&sig=random"
            );
        }

        // see com.azure.storage.blob.BlobUrlParts.parseIpUrl
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "/" + accountName;
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
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
        AzureStorageService createAzureStorageService(Settings settingsToUse, AzureClientProvider azureClientProvider) {
            return new AzureStorageService(settingsToUse, azureClientProvider) {
                @Override
                RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                    return new RequestRetryOptions(
                        RetryPolicyType.EXPONENTIAL,
                        azureStorageSettings.getMaxRetries() + 1,
                        60,
                        50L,
                        100L,
                        null
                    );
                }

                @Override
                long getUploadBlockSize() {
                    return ByteSizeUnit.MB.toBytes(1);
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an Azure endpoint")
    private static class AzureBlobStoreHttpHandler extends AzureHttpHandler implements BlobStoreHttpHandler {

        AzureBlobStoreHttpHandler(final String account, final String container) {
            super(account, container);
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
            try {
                drainInputStream(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            } finally {
                exchange.close();
            }
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            final String requestId = exchange.getRequestHeaders().getFirst("X-ms-client-request-id");
            final String range = exchange.getRequestHeaders().getFirst("Content-Range");
            return exchange.getRequestMethod() + " " + requestId + (range != null ? " " + range : "");
        }
    }

    /**
     * HTTP handler that keeps track of requests performed against Azure Storage.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureHTTPStatsCollectorHandler extends HttpStatsCollectorHandler {
        private static final Pattern LIST_PATTERN = Pattern.compile("GET /[a-zA-Z0-9]+/[a-zA-Z0-9]+\\?.+");
        private static final Pattern GET_BLOB_PATTERN = Pattern.compile("GET /[a-zA-Z0-9]+/[a-zA-Z0-9]+/.+");

        private AzureHTTPStatsCollectorHandler(HttpHandler delegate) {
            super(delegate);
        }

        @Override
        protected void maybeTrack(String request, Headers headers) {
            if (GET_BLOB_PATTERN.matcher(request).matches()) {
                trackRequest("GetBlob");
            } else if (Regex.simpleMatch("HEAD /*/*/*", request)) {
                trackRequest("GetBlobProperties");
            } else if (LIST_PATTERN.matcher(request).matches()) {
                trackRequest("ListBlobs");
            } else if (isPutBlock(request)) {
                trackRequest("PutBlock");
            } else if (isPutBlockList(request)) {
                trackRequest("PutBlockList");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutBlob");
            }
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
        private boolean isPutBlock(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=block*", request) && request.contains("blockid=");
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        private boolean isPutBlockList(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=blocklist*", request);
        }
    }

    public void testLargeBlobCountDeletion() throws Exception {
        int numberOfBlobs = randomIntBetween(257, 2000);
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            for (int i = 0; i < numberOfBlobs; i++) {
                byte[] bytes = randomBytes(randomInt(100));
                String blobName = randomAlphaOfLength(10);
                container.writeBlob(blobName, new BytesArray(bytes), false);
            }

            container.delete();
            assertThat(container.listBlobs(), is(anEmptyMap()));
        }
    }

    public void testDeleteBlobsIgnoringIfNotExists() throws Exception {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            List<String> blobsToDelete = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                byte[] bytes = randomBytes(randomInt(100));
                String blobName = randomAlphaOfLength(10);
                container.writeBlob(blobName, new BytesArray(bytes), false);
                blobsToDelete.add(blobName);
            }

            // Try to delete non existent blobs
            for (int i = 0; i < 10; i++) {
                blobsToDelete.add(randomName());
            }

            Randomness.shuffle(blobsToDelete);
            container.deleteBlobsIgnoringIfNotExists(blobsToDelete.iterator());
            assertThat(container.listBlobs(), is(anEmptyMap()));
        }
    }

    public void testNotFoundErrorMessageContainsFullKey() throws Exception {
        try (BlobStore store = newBlobStore()) {
            BlobContainer container = store.blobContainer(BlobPath.EMPTY.add("nested").add("dir"));
            NoSuchFileException exception = expectThrows(NoSuchFileException.class, () -> container.readBlob("blob"));
            assertThat(exception.getMessage(), containsString("nested/dir/blob] not found"));
        }
    }

    public void testReadByteByByte() throws Exception {
        try (BlobStore store = newBlobStore()) {
            BlobContainer container = store.blobContainer(BlobPath.EMPTY.add(UUIDs.randomBase64UUID()));
            byte[] data = randomBytes(randomIntBetween(128, 512));
            String blobName = randomName();
            container.writeBlob(blobName, new ByteArrayInputStream(data), data.length, true);

            InputStream originalDataInputStream = new ByteArrayInputStream(data);
            try (InputStream azureInputStream = container.readBlob(blobName)) {
                for (int i = 0; i < data.length; i++) {
                    assertThat(originalDataInputStream.read(), is(equalTo(azureInputStream.read())));
                }

                assertThat(azureInputStream.read(), is(equalTo(-1)));
                assertThat(originalDataInputStream.read(), is(equalTo(-1)));
            }
            container.delete();
        }
    }
}
