/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpHandler;

import com.amazonaws.http.AmazonHttpClient;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
// Need to set up a new cluster for each test because cluster settings use randomized authentication settings
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class S3BlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    private static final TimeValue TEST_COOLDOWN_PERIOD = TimeValue.timeValueSeconds(10L);

    private String region;
    private String signerOverride;

    @Override
    public void setUp() throws Exception {
        if (randomBoolean()) {
            region = "test-region";
        }
        if (region != null && randomBoolean()) {
            signerOverride = randomFrom("AWS3SignerType", "AWS4SignerType");
        } else if (randomBoolean()) {
            signerOverride = "AWS3SignerType";
        }
        super.setUp();
    }

    @Override
    protected String repositoryType() {
        return S3Repository.TYPE;
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.repositorySettings(repoName))
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            .put(S3Repository.CLIENT_NAME.getKey(), "test")
            // Don't cache repository data because some tests manually modify the repository data
            .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false);
        if (randomBoolean()) {
            settingsBuilder.put(S3Repository.BASE_PATH_SETTING.getKey(), randomFrom("test", "test/1"));
        }
        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestS3RepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/bucket", new S3StatsCollectorHttpHandler(new S3BlobStoreHttpHandler("bucket")));
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new S3StatsCollectorHttpHandler(new S3ErroneousHttpHandler(delegate, randomIntBetween(2, 3)));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "test_access_key");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "test_secret_key");

        final Settings.Builder builder = Settings.builder()
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that verify an exact wait time
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl())
            // Disable request throttling because some random values in tests might generate too many failures for the S3 client
            .put(S3ClientSettings.USE_THROTTLE_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), false)
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .setSecureSettings(secureSettings);

        if (randomBoolean()) {
            builder.put(S3ClientSettings.DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace("test").getKey(), randomBoolean());
        }
        if (signerOverride != null) {
            builder.put(S3ClientSettings.SIGNER_OVERRIDE.getConcreteSettingForNamespace("test").getKey(), signerOverride);
        }
        if (region != null) {
            builder.put(S3ClientSettings.REGION.getConcreteSettingForNamespace("test").getKey(), region);
        }
        return builder.build();
    }

    @Override
    @TestLogging(reason = "Enable request logging to debug #88841", value = "com.amazonaws.request:DEBUG")
    public void testRequestStats() throws Exception {
        super.testRequestStats();
    }

    public void testEnforcedCooldownPeriod() throws IOException {
        final String repoName = randomRepositoryName();
        createRepository(
            repoName,
            Settings.builder().put(repositorySettings(repoName)).put(S3Repository.COOLDOWN_PERIOD.getKey(), TEST_COOLDOWN_PERIOD).build(),
            true
        );

        final SnapshotId fakeOldSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-old")
            .setWaitForCompletion(true)
            .setIndices()
            .get()
            .getSnapshotInfo()
            .snapshotId();
        final RepositoriesService repositoriesService = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        final RepositoryData modifiedRepositoryData = repositoryData.withoutUUIDs()
            .withExtraDetails(
                Collections.singletonMap(
                    fakeOldSnapshot,
                    new RepositoryData.SnapshotDetails(
                        SnapshotState.SUCCESS,
                        SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION.minimumCompatibilityVersion().indexVersion,
                        0L, // -1 would refresh RepositoryData and find the real version
                        0L, // -1 would refresh RepositoryData and find the real version,
                        "" // null would refresh RepositoryData and find the real version
                    )
                )
            );
        final BytesReference serialized = BytesReference.bytes(
            modifiedRepositoryData.snapshotsToXContent(XContentFactory.jsonBuilder(), SnapshotsService.OLD_SNAPSHOT_FORMAT)
        );
        PlainActionFuture.get(
            f -> repository.threadPool()
                .generic()
                .execute(
                    ActionRunnable.run(
                        f,
                        () -> repository.blobStore()
                            .blobContainer(repository.basePath())
                            .writeBlobAtomic(BlobStoreRepository.INDEX_FILE_PREFIX + modifiedRepositoryData.getGenId(), serialized, true)
                    )
                )
        );

        final String newSnapshotName = "snapshot-new";
        final long beforeThrottledSnapshot = repository.threadPool().relativeTimeInNanos();
        client().admin().cluster().prepareCreateSnapshot(repoName, newSnapshotName).setWaitForCompletion(true).setIndices().get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeThrottledSnapshot, greaterThan(TEST_COOLDOWN_PERIOD.getNanos()));

        final long beforeThrottledDelete = repository.threadPool().relativeTimeInNanos();
        client().admin().cluster().prepareDeleteSnapshot(repoName, newSnapshotName).get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeThrottledDelete, greaterThan(TEST_COOLDOWN_PERIOD.getNanos()));

        final long beforeFastDelete = repository.threadPool().relativeTimeInNanos();
        client().admin().cluster().prepareDeleteSnapshot(repoName, fakeOldSnapshot.getName()).get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeFastDelete, lessThan(TEST_COOLDOWN_PERIOD.getNanos()));
    }

    /**
     * S3RepositoryPlugin that allows to disable chunked encoding and to set a low threshold between single upload and multipart upload.
     */
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

        @Override
        protected S3Repository createRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return new S3Repository(metadata, registry, getService(), clusterService, bigArrays, recoverySettings) {

                @Override
                public BlobStore blobStore() {
                    return new BlobStoreWrapper(super.blobStore()) {
                        @Override
                        public BlobContainer blobContainer(final BlobPath path) {
                            return new S3BlobContainer(path, (S3BlobStore) delegate()) {
                                @Override
                                long getLargeBlobThresholdInBytes() {
                                    return ByteSizeUnit.MB.toBytes(1L);
                                }

                                @Override
                                void ensureMultiPartUploadSize(long blobSize) {}
                            };
                        }
                    };
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an S3 endpoint")
    private class S3BlobStoreHttpHandler extends S3HttpHandler implements BlobStoreHttpHandler {

        S3BlobStoreHttpHandler(final String bucket) {
            super(bucket);
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            validateAuthHeader(exchange);
            super.handle(exchange);
        }

        private void validateAuthHeader(HttpExchange exchange) {
            final String authorizationHeaderV4 = exchange.getRequestHeaders().getFirst("Authorization");
            final String authorizationHeaderV3 = exchange.getRequestHeaders().getFirst("X-amzn-authorization");

            if ("AWS3SignerType".equals(signerOverride)) {
                assertThat(authorizationHeaderV3, startsWith("AWS3"));
            } else if ("AWS4SignerType".equals(signerOverride)) {
                assertThat(authorizationHeaderV4, containsString("aws4_request"));
            }
            if (region != null && authorizationHeaderV4 != null) {
                assertThat(authorizationHeaderV4, containsString("/" + region + "/s3/"));
            }
        }
    }

    /**
     * HTTP handler that injects random S3 service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class S3ErroneousHttpHandler extends ErroneousHttpHandler {

        S3ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            // Amazon SDK client provides a unique ID per request
            return exchange.getRequestHeaders().getFirst(AmazonHttpClient.HEADER_SDK_TRANSACTION_ID);
        }
    }

    /**
     * HTTP handler that tracks the number of requests performed against S3.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class S3StatsCollectorHttpHandler extends HttpStatsCollectorHandler {

        S3StatsCollectorHttpHandler(final HttpHandler delegate) {
            super(delegate);
        }

        @Override
        public void maybeTrack(final String request, Headers requestHeaders) {
            if (Regex.simpleMatch("GET /*/?prefix=*", request)) {
                trackRequest("ListObjects");
            } else if (Regex.simpleMatch("GET /*/*", request)) {
                trackRequest("GetObject");
            } else if (isMultiPartUpload(request)) {
                trackRequest("PutMultipartObject");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutObject");
            }
        }

        private boolean isMultiPartUpload(String request) {
            return Regex.simpleMatch("POST /*/*?uploads", request)
                || Regex.simpleMatch("POST /*/*?*uploadId=*", request)
                || Regex.simpleMatch("PUT /*/*?*uploadId=*", request);
        }
    }
}
