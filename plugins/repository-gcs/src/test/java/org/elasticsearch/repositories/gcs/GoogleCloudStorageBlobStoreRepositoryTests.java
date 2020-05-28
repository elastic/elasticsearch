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
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(BUCKET.getKey(), "bucket")
            .put(CLIENT_NAME.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestGoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of(
            "/", new GoogleCloudStorageStatsCollectorHttpHandler(new GoogleCloudStorageBlobStoreHttpHandler("bucket")),
            "/token", new FakeOAuth2HttpHandler()
        );
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new GoogleErroneousHttpHandler(delegate, randomIntBetween(2, 3));
    }

    @Override
    protected List<String> requestTypesTracked() {
        return List.of("GET", "LIST", "POST", "PUT");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.nodeSettings(nodeOrdinal));
        settings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl());
        settings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl() + "/token");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        final byte[] serviceAccount = TestUtils.createServiceAccount(random());
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(), serviceAccount);
        settings.setSecureSettings(secureSettings);
        return settings.build();
    }

    public void testDeleteSingleItem() {
        final String repoName = createRepository(randomName());
        final RepositoriesService repositoriesService = internalCluster().getMasterNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        PlainActionFuture.get(f -> repository.threadPool().generic().execute(ActionRunnable.run(f, () ->
            repository.blobStore().blobContainer(repository.basePath()).deleteBlobsIgnoringIfNotExists(Collections.singletonList("foo"))
        )));
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
                                                                        Settings.builder().put("chunk_size", "101mb").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [101mb] for setting [chunk_size], must be <= [100mb]", e.getMessage());
    }

    public void testWriteReadLarge() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
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
                                                               ClusterService clusterService) {
            return Collections.singletonMap(GoogleCloudStorageRepository.TYPE,
                metadata -> new GoogleCloudStorageRepository(metadata, registry, this.storageService, clusterService) {
                    @Override
                    protected GoogleCloudStorageBlobStore createBlobStore() {
                        return new GoogleCloudStorageBlobStore("bucket", "test", metadata.name(), storageService) {
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
            if (Regex.simpleMatch("GET /storage/v1/b/*/o*", request)) {
                trackRequest("LIST");
            } else if (Regex.simpleMatch("GET /download/storage/v1/b/*", request)) {
                trackRequest("GET");
            } else if (Regex.simpleMatch("PUT /upload/storage/v1/b/*", request) && isLastPart(requestHeaders)) {
                trackRequest("PUT");
            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/*uploadType=multipart*", request)) {
                trackRequest("POST");
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
