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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.RESUMABLE_WRITE_BUFFER_SIZE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BASE_PATH;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

/// Shared infrastructure for the GCS blob-store repository integration tests: the mock GCS
/// HTTP handlers, the erroneous (fault-injecting) handler, the node/repository settings and the
/// test [GoogleCloudStoragePlugin]. It is deliberately `abstract` and declares no `@Test` methods
/// so the runner never instantiates it and concrete subclasses do not double-run inherited tests.
///
/// Concrete subclasses tune behaviour through three small hooks:
///  - [#resumableWriteBufferSize()] pins the resumable-write buffer size (default: production 16MB).
///  - [#maybeWrapForRecording(HttpHandler)] optionally wraps the blob handler to observe chunk sizes.
///  - [#suppressErrorInjection()] disables fault injection (used while recording exact chunk sizes).
///
/// The split exists because [GoogleCloudStorageResumableWriteBufferTests] needs a 1mb buffer to assert
/// exact chunk sizes, but that pin turned other tests' large uploads into dozens of tiny chunks and
/// amplified injected-error retry storms (see #152286).
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public abstract class AbstractGoogleCloudStorageBlobStoreRepositoryTestCase extends ESMockAPIBasedRepositoryIntegTestCase {

    private static final String CLIENT_ID_HEADER = "x-es-test-client-id";

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

    /// The resumable-write buffer size to pin for the "test" client, or [Optional#empty()] to keep the
    /// production default (16MB). Overridden by the buffer test that needs a deterministic chunk size.
    protected Optional<ByteSizeValue> resumableWriteBufferSize() {
        return Optional.empty();
    }

    /// Hook to optionally wrap the blob-store handler, e.g. to record uploaded chunk sizes. The default
    /// returns the handler unchanged; keeping the handler classes private to this base class.
    protected HttpHandler maybeWrapForRecording(HttpHandler blobStoreHandler) {
        return blobStoreHandler;
    }

    /// Whether the erroneous handler should skip injecting errors. Overridden while recording chunk
    /// sizes so the GCS SDK never issues a resumable-upload status check against the mock server.
    protected boolean suppressErrorInjection() {
        return false;
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        final HttpHandler blob = maybeWrapForRecording(new GoogleCloudStorageBlobStoreHttpHandler("bucket"));
        return Map.of("/", new GoogleCloudStorageStatsCollectorHttpHandler(blob), "/token", new FakeOAuth2HttpHandler());
    }

    // GCP Oauth2 client uses own retries: 1-second initial delay, 3-tries, 2x multiplier
    // it can take long time to pass through(>10s) from multiple nodes, and trip tests with timeouts
    // https://github.com/googleapis/google-auth-library-java/blob/main/oauth2_http/java/com/google/auth/oauth2/OAuth2Utils.java#L115-L118
    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        if (delegate instanceof FakeOAuth2HttpHandler) {
            return new GoogleErroneousHttpHandler(delegate, 1);
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
        settings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), 6);
        // Multiple nodes (6 for nightly) with up to 2-3 injected failures per request and only 2 threads for the HTTP
        // mock GCS server can lead to timeouts when reading the response after writing (or draining) large buffers
        // (up to the 16MB production default). Kept defensively; reducing it is out of scope.
        settings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace("test").getKey(), "60s");
        // Subclasses may pin the resumable-write buffer size (e.g. to assert exact chunk sizes); by default the
        // production 16MB default is left in place so uploads are not shredded into many tiny chunks.
        resumableWriteBufferSize().ifPresent(
            size -> settings.put(RESUMABLE_WRITE_BUFFER_SIZE_SETTING.getConcreteSettingForNamespace("test").getKey(), size)
        );

        final MockSecureSettings secureSettings = new MockSecureSettings();
        final byte[] serviceAccount = TestUtils.createServiceAccount(random());
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(), serviceAccount);
        settings.setSecureSettings(secureSettings);
        return settings.build();
    }

    public static class TestGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {

        public TestGoogleCloudStoragePlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GoogleCloudStorageService createStorageService(ClusterService clusterService, ProjectResolver projectResolver) {
            // We generate the client ID here, otherwise it gets generated on each nodes' snapshot thread(s)
            // and the chance of collision is greatly increased because of deterministic randomness
            final String clientId = randomUUID();
            return new GoogleCloudStorageService(clusterService, projectResolver) {
                @Override
                StorageOptions createStorageOptions(
                    final GoogleCloudStorageClientSettings gcsClientSettings,
                    final HttpTransportOptions httpTransportOptions
                ) {
                    StorageOptions options = super.createStorageOptions(gcsClientSettings, httpTransportOptions);
                    return options.toBuilder()
                        .setStorageRetryStrategy(StorageRetryStrategy.getLegacyStorageRetryStrategy())
                        .setRetrySettings(
                            options.getRetrySettings()
                                .toBuilder()
                                .setInitialRetryDelay(Duration.ofMillis(10L))
                                .setMaxRetryDelay(Duration.ofSeconds(1L))
                                // Tests use flat retry backoff (1.0) instead of the SDK default exponential (2.0). Combined
                                // with the small initial/max delays above this keeps injected-error retries fast and
                                // deterministic; exponential backoff let accumulated sleeps across many retried chunk
                                // uploads exceed the 60s read timeout under CI contention (#152286). Production keeps the
                                // SDK default via ServiceOptions.getDefaultRetrySettings().
                                .setRetryDelayMultiplier(1.0d)
                                .setJittered(false)
                                .build()
                        )
                        .setHeaderProvider(new HeaderProvider() {
                            /**
                             * GCS client doesn't implement any way of identifying the client making the request.
                             * Adding this header makes it easier for us to know how many times it's safe to fail
                             * a request without exceeding the configured max retries.
                             */
                            @Override
                            public Map<String, String> getHeaders() {
                                return Map.of(CLIENT_ID_HEADER, clientId);
                            }
                        })
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
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Collections.singletonMap(
                GoogleCloudStorageRepository.TYPE,
                (projectId, metadata) -> new GoogleCloudStorageRepository(
                    projectId,
                    metadata,
                    registry,
                    this.storageService.get(),
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    new GcsRepositoryStatsCollector(),
                    snapshotMetrics
                ) {
                    @Override
                    protected GoogleCloudStorageBlobStore createBlobStore() {
                        return new GoogleCloudStorageBlobStore(
                            getProjectId(),
                            metadata.settings().get("bucket"),
                            "test",
                            metadata.name(),
                            storageService.get(),
                            bigArrays,
                            randomIntBetween(1, 8) * 1024,
                            BackoffPolicy.noBackoff(),
                            this.statsCollector(),
                            null,
                            null
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

        @Override
        public Set<String> blobsKeyset() {
            return blobs().keySet();
        }
    }

    /**
     * HTTP handler that injects random  Google Cloud Storage service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
    private class GoogleErroneousHttpHandler extends ErroneousHttpHandler {

        private static final Logger logger = LogManager.getLogger(GoogleErroneousHttpHandler.class);
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

            String clientId = exchange.getRequestHeaders().getFirst(CLIENT_ID_HEADER);
            if (clientId == null) {
                if (exchange.getRequestURI().toString().startsWith("/batch/") == false) {
                    final String message = Strings.format(
                        "Missing %s on non-batch request, this may cause issues with fault injection: %s",
                        CLIENT_ID_HEADER,
                        exchange.getRequestURI()
                    );
                    ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(message));
                }
                clientId = exchange.getRemoteAddress().toString();
            }
            final String range = exchange.getRequestHeaders().getFirst("Content-Range");
            return clientId + " " + exchange.getRequestMethod() + " " + exchange.getRequestURI() + (range != null ? " " + range : "");
        }

        @Override
        protected boolean canFailRequest(final HttpExchange exchange) {
            // Subclasses can suppress error injection (e.g. while recording chunk sizes) so the GCS SDK
            // never needs to issue a resumable-upload status check against the mock server.
            if (suppressErrorInjection()) {
                return false;
            }
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
            } else if (Regex.simpleMatch("POST /storage/v1/b/*/o/*/rewriteTo/b/*/o/*", request)) {
                trackRequest(StorageOperation.COPY.key());
            }
        }
    }
}
