/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.junit.BeforeClass;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BASE_PATH;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    public static void assumeNotJava8() {
        assumeFalse("This test is flaky on jdk8 - we suspect a JDK bug to trigger some assertion in the HttpServer implementation used " +
            "to emulate the server side logic of Google Cloud Storage. See https://bugs.openjdk.java.net/browse/JDK-8180754, " +
            "https://github.com/elastic/elasticsearch/pull/51933 and https://github.com/elastic/elasticsearch/issues/52906 " +
            "for more background on this issue.", JavaVersion.current().equals(JavaVersion.parse("8")));
    }

    @BeforeClass
    public static void skipJava8() {
        assumeNotJava8();
    }

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
        final Map<String, HttpHandler> handlers = new HashMap<>(2);
        handlers.put("/", new GoogleCloudStorageStatsCollectorHttpHandler(new GoogleCloudStorageBlobStoreHttpHandler("bucket")));
        handlers.put("/token", new FakeOAuth2HttpHandler());
        return Collections.unmodifiableMap(handlers);
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new GoogleErroneousHttpHandler(delegate, randomIntBetween(2, 3));
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

    public void testDeleteSingleItem() {
        final String repoName = createRepository(randomRepositoryName());
        final RepositoriesService repositoriesService = internalCluster().getMasterNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        PlainActionFuture.get(f -> repository.threadPool().generic().execute(ActionRunnable.run(f, () ->
            repository.blobStore().blobContainer(repository.basePath()).deleteBlobsIgnoringIfNotExists(Iterators.single("foo")))));
    }

    public void testChunkSize() {
        // default chunk size
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY);
        ByteSizeValue chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(GoogleCloudStorageRepository.MAX_CHUNK_SIZE, chunkSize);

        // chunk size in settings
        final int size = randomIntBetween(1, 100);
        repositoryMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE,
                                                       Settings.builder().put("chunk_size", size + "mb").build());
        chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(new ByteSizeValue(size, ByteSizeUnit.MB), chunkSize);

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "0").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "-1").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "6tb").build());
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
            try (InputStream stream = container.readBlob("foobar")) {
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
            container.delete();
        }
    }

    public static class TestGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {

        public TestGoogleCloudStoragePlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GoogleCloudStorageService createStorageService() {
            return new GoogleCloudStorageService() {
                @Override
                StorageOptions createStorageOptions(final GoogleCloudStorageClientSettings clientSettings,
                                                    final HttpTransportOptions httpTransportOptions) {
                    StorageOptions options = super.createStorageOptions(clientSettings, httpTransportOptions);
                    return options.toBuilder()
                        .setHost(options.getHost())
                        .setCredentials(options.getCredentials())
                        .setRetrySettings(RetrySettings.newBuilder()
                            .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                            .setInitialRetryDelay(Duration.ofMillis(10L))
                            .setRetryDelayMultiplier(options.getRetrySettings().getRetryDelayMultiplier())
                            .setMaxRetryDelay(Duration.ofSeconds(1L))
                            .setMaxAttempts(0)
                            .setJittered(false)
                            .setInitialRpcTimeout(options.getRetrySettings().getInitialRpcTimeout())
                            .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                            .setMaxRpcTimeout(options.getRetrySettings().getMaxRpcTimeout())
                            .build())
                        .build();
                }
            };
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry registry,
                                                               ClusterService clusterService, BigArrays bigArrays,
                                                               RecoverySettings recoverySettings) {
            return Collections.singletonMap(GoogleCloudStorageRepository.TYPE,
                metadata -> new GoogleCloudStorageRepository(metadata, registry, this.storageService, clusterService,
                        bigArrays, recoverySettings) {
                    @Override
                    protected GoogleCloudStorageBlobStore createBlobStore() {
                        return new GoogleCloudStorageBlobStore(
                                metadata.settings().get("bucket"), "test", metadata.name(), storageService, bigArrays,
                            randomIntBetween(1, 8) * 1024) {
                            @Override
                            long getLargeBlobThresholdInBytes() {
                                return ByteSizeUnit.MB.toBytes(1);
                            }
                        };
                    }
                });
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

            final String range = exchange.getRequestHeaders().getFirst("Content-Range");
            return exchange.getRemoteAddress().getHostString()
                + " " + exchange.getRequestMethod()
                + " " + exchange.getRequestURI()
                + (range != null ?  " " + range :  "");
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

        public static final Pattern contentRangeMatcher = Pattern.compile("bytes \\d+-(\\d+)/(\\d+)");

        GoogleCloudStorageStatsCollectorHttpHandler(final HttpHandler delegate) {
            super(delegate);
        }

        @Override
        public void maybeTrack(final String request, Headers requestHeaders) {
            if (Regex.simpleMatch("GET /storage/v1/b/*/o/*", request)) {
                trackRequest("GetObject");
            } else if (Regex.simpleMatch("GET /storage/v1/b/*/o*", request)) {
                trackRequest("ListObjects");
            } else if (Regex.simpleMatch("GET /download/storage/v1/b/*", request)) {
                trackRequest("GetObject");
            } else if (Regex.simpleMatch("PUT /upload/storage/v1/b/*uploadType=resumable*", request) && isLastPart(requestHeaders)) {
                // Resumable uploads are billed as a single operation, that's the reason we're tracking
                // the request only when it's the last part.
                // See https://cloud.google.com/storage/docs/resumable-uploads#introduction
                trackRequest("InsertObject");
            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/*uploadType=multipart*", request)) {
                trackRequest("InsertObject");
            }
        }

        boolean isLastPart(Headers requestHeaders) {
            if (requestHeaders.containsKey("Content-range") == false)
                return false;

            // https://cloud.google.com/storage/docs/json_api/v1/parameters#contentrange
            final String contentRange = requestHeaders.getFirst("Content-range");

            final Matcher matcher = contentRangeMatcher.matcher(contentRange);

            if (matcher.matches() == false)
                return false;

            String upperBound = matcher.group(1);
            String totalLength = matcher.group(2);
            return Integer.parseInt(upperBound) == Integer.parseInt(totalLength) - 1;
        }
    }
}
