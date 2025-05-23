/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.io.Streams.readFully;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BASE_PATH;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.repositorySettings(repoName))
            .put(BUCKET.getKey(), "bucket")
            .put(CLIENT_NAME.getKey(), "test");
        if (randomBoolean()) {
            settingsBuilder.put(BASE_PATH.getKey(), randomFrom("test", "test/1"));
        }
        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestGoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of(
            "/",
            new GoogleCloudStorageStatsCollectorHttpHandler(new GoogleCloudStorageBlobStoreHttpHandler("bucket")),
            "/token",
            new FakeOAuth2HttpHandler()
        );
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        if (delegate instanceof FakeOAuth2HttpHandler) {
            return new GoogleErroneousHttpHandler(delegate, randomIntBetween(2, 3));
        } else {
            return new GoogleCloudStorageStatsCollectorHttpHandler(new GoogleErroneousHttpHandler(delegate, randomIntBetween(2, 3)));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl());
        settings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl() + "/token");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        final byte[] serviceAccount = TestUtils.createServiceAccount(random());
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(), serviceAccount);
        settings.setSecureSettings(secureSettings);
        return settings.build();
    }

    public void testDeleteSingleItem() throws IOException {
        final String repoName = createRepository(randomRepositoryName());
        final RepositoriesService repositoriesService = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        repository.blobStore()
            .blobContainer(repository.basePath())
            .deleteBlobsIgnoringIfNotExists(randomPurpose(), Iterators.single("foo"));
    }

    public void testChunkSize() {
        // default chunk size
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY);
        ByteSizeValue chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(GoogleCloudStorageRepository.MAX_CHUNK_SIZE, chunkSize);

        // chunk size in settings
        final int size = randomIntBetween(1, 100);
        repositoryMetadata = new RepositoryMetadata(
            "repo",
            GoogleCloudStorageRepository.TYPE,
            Settings.builder().put("chunk_size", size + "mb").build()
        );
        chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(ByteSizeValue.of(size, ByteSizeUnit.MB), chunkSize);

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "0").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "-1").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "6tb").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [6tb] for setting [chunk_size], must be <= [5tb]", e.getMessage());
    }

    public void testWriteReadLarge() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            byte[] data = randomBytes(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE + 1);
            writeBlob(container, "foobar", new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                random().nextBytes(data);
                writeBlob(container, "foobar", new BytesArray(data), false);
            }
            try (InputStream stream = container.readBlob(randomPurpose(), "foobar")) {
                BytesRefBuilder target = new BytesRefBuilder();
                while (target.length() < data.length) {
                    byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = stream.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                }
                assertEquals(data.length, target.length());
                assertArrayEquals(data, Arrays.copyOfRange(target.bytes(), 0, target.length()));
            }
            container.delete(randomPurpose());
        }
    }

    public void testWriteFileMultipleOfChunkSize() throws IOException {
        final int uploadSize = randomIntBetween(2, 4) * GoogleCloudStorageBlobStore.SDK_DEFAULT_CHUNK_SIZE;
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            final String key = randomIdentifier();
            byte[] initialValue = randomByteArrayOfLength(uploadSize);
            container.writeBlob(randomPurpose(), key, new BytesArray(initialValue), true);

            BytesReference reference = readFully(container.readBlob(randomPurpose(), key));
            assertEquals(new BytesArray(initialValue), reference);

            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), Iterators.single(key));
        }
    }

    @Override
    public void testRequestStats() throws Exception {
        super.testRequestStats();
    }

    public static class TestGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {

        public TestGoogleCloudStoragePlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GoogleCloudStorageService createStorageService(boolean isServerless) {
            return new GoogleCloudStorageService() {
                @Override
                StorageOptions createStorageOptions(
                    final GoogleCloudStorageClientSettings gcsClientSettings,
                    final HttpTransportOptions httpTransportOptions
                ) {
                    StorageOptions options = super.createStorageOptions(gcsClientSettings, httpTransportOptions);
                    return options.toBuilder()
                        .setStorageRetryStrategy(StorageRetryStrategy.getLegacyStorageRetryStrategy())
                        .setHost(options.getHost())
                        .setCredentials(options.getCredentials())
                        .setRetrySettings(
                            RetrySettings.newBuilder()
                                .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                                .setInitialRetryDelay(Duration.ofMillis(10L))
                                .setRetryDelayMultiplier(options.getRetrySettings().getRetryDelayMultiplier())
                                .setMaxRetryDelay(Duration.ofSeconds(1L))
                                .setMaxAttempts(0)
                                .setJittered(false)
                                .setInitialRpcTimeout(options.getRetrySettings().getInitialRpcTimeout())
                                .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                                .setMaxRpcTimeout(options.getRetrySettings().getMaxRpcTimeout())
                                .build()
                        )
                        .build();
                }
            };
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                GoogleCloudStorageRepository.TYPE,
                (projectId, metadata) -> new GoogleCloudStorageRepository(
                    metadata,
                    registry,
                    this.storageService,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    new GcsRepositoryStatsCollector()
                ) {
                    @Override
                    protected GoogleCloudStorageBlobStore createBlobStore() {
                        return new GoogleCloudStorageBlobStore(
                            metadata.settings().get("bucket"),
                            "test",
                            metadata.name(),
                            storageService,
                            bigArrays,
                            randomIntBetween(1, 8) * 1024,
                            BackoffPolicy.noBackoff(),
                            this.statsCollector()
                        ) {
                            @Override
                            long getLargeBlobThresholdInBytes() {
                                return ByteSizeUnit.MB.toBytes(1);
                            }
                        };
                    }
                }
            );
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate a Google Cloud Storage endpoint")
    private static class GoogleCloudStorageBlobStoreHttpHandler extends GoogleCloudStorageHttpHandler implements BlobStoreHttpHandler {

        GoogleCloudStorageBlobStoreHttpHandler(final String bucket) {
            super(bucket);
        }
    }

    /**
     * HTTP handler that injects random  Google Cloud Storage service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
    private static class GoogleErroneousHttpHandler extends ErroneousHttpHandler {

        private static final String IDEMPOTENCY_TOKEN = "x-goog-gcs-idempotency-token";

        GoogleErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected String requestUniqueId(HttpExchange exchange) {
            if ("/token".equals(exchange.getRequestURI().getPath())) {
                try {
                    // token content is unique per node (not per request)
                    return Streams.readFully(Streams.noCloseStream(exchange.getRequestBody())).utf8ToString();
                } catch (IOException e) {
                    throw new AssertionError("Unable to read token request body", e);
                }
            }

            if (exchange.getRequestHeaders().containsKey(IDEMPOTENCY_TOKEN)) {
                String idempotencyToken = exchange.getRequestHeaders().getFirst(IDEMPOTENCY_TOKEN);
                // In the event of a resumable retry, the GCS client uses the same idempotency token for
                // the retry status check and the subsequent retries.
                // Including the range header allows us to disambiguate between the requests
                // see https://github.com/googleapis/java-storage/issues/3040
                if (exchange.getRequestHeaders().containsKey("Content-Range")) {
                    idempotencyToken += " " + exchange.getRequestHeaders().getFirst("Content-Range");
                }
                return idempotencyToken;
            }

            final String range = exchange.getRequestHeaders().getFirst("Content-Range");
            return exchange.getRemoteAddress().getHostString()
                + " "
                + exchange.getRequestMethod()
                + " "
                + exchange.getRequestURI()
                + (range != null ? " " + range : "");
        }

        @Override
        protected boolean canFailRequest(final HttpExchange exchange) {
            // Batch requests are not retried so we don't want to fail them
            // The batched request are supposed to be retried (not tested here)
            return exchange.getRequestURI().toString().startsWith("/batch/") == false;
        }
    }

    /**
     * HTTP handler that keeps track of requests performed against GCP.
     */
    @SuppressForbidden(reason = "this tests uses a HttpServer to emulate an GCS endpoint")
    private static class GoogleCloudStorageStatsCollectorHttpHandler extends HttpStatsCollectorHandler {

        GoogleCloudStorageStatsCollectorHttpHandler(final HttpHandler delegate) {
            super(delegate, Arrays.stream(StorageOperation.values()).map(StorageOperation::key).toArray(String[]::new));
        }

        @Override
        public void maybeTrack(HttpExchange exchange) {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            if (Regex.simpleMatch("GET */storage/v1/b/*/o/*", request)) {
                trackRequest(StorageOperation.GET.key());
            } else if (Regex.simpleMatch("GET /storage/v1/b/*/o*", request)) {
                trackRequest(StorageOperation.LIST.key());
            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/*uploadType=resumable*", request)) {
                trackRequest(StorageOperation.INSERT.key());
            } else if (Regex.simpleMatch("PUT /upload/storage/v1/b/*uploadType=resumable*", request)) {
                trackRequest(StorageOperation.INSERT.key());
            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/*uploadType=multipart*", request)) {
                trackRequest(StorageOperation.INSERT.key());
            }
        }
    }
}
