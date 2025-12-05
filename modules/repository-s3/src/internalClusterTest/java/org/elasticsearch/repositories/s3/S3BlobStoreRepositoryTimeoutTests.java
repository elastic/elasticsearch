/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpHandler;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.repositories.s3.S3BlobStore.Operation;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_EXCEPTIONS_HISTOGRAM;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
// Need to set up a new cluster for each test because cluster settings use randomized authentication settings
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class S3BlobStoreRepositoryTimeoutTests extends ESMockAPIBasedRepositoryIntegTestCase {

    private S3StallingHttpHandler s3StallingHttpHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(S3RepositoryPlugin.class, TestTelemetryPlugin.class);
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
            .put(S3Repository.CLIENT_NAME.getKey(), "test");
        if (randomBoolean()) {
            settingsBuilder.put(S3Repository.BASE_PATH_SETTING.getKey(), randomFrom("test", "test/1"));
        }
        return settingsBuilder.build();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        this.s3StallingHttpHandler = new S3StallingHttpHandler("bucket");
        return Collections.singletonMap("/bucket", this.s3StallingHttpHandler);
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return delegate;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "test_access_key");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "test_secret_key");

        final Settings.Builder builder = Settings.builder()
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that verify an exact wait time
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl())
            .put(S3ClientSettings.READ_TIMEOUT_SETTING.getConcreteSettingForNamespace("test").getKey(), "1s")
            .put(S3ClientSettings.MAX_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), "0")
            .put(S3ClientSettings.API_CALL_TIMEOUT_SETTING.getConcreteSettingForNamespace("test").getKey(), "5s")
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .setSecureSettings(secureSettings);

        return builder.build();
    }

    public void testWriteTimeout() {
        final String repository = createRepository(randomIdentifier());

        final var dataNodeName = internalCluster().getRandomDataNodeName();
        final var blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(ProjectId.DEFAULT, repository);
        final var blobContainer = blobStoreRepository.blobStore().blobContainer(BlobPath.EMPTY.add(randomIdentifier()));
        final var plugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        final var latch = new CountDownLatch(1);
        s3StallingHttpHandler.setStallLatchRef(latch);
        try {
            final var purpose = randomFrom(OperationPurpose.values());
            final String blobName = "test-" + randomIdentifier();
            assert BlobContainer.assertPurposeConsistency(purpose, blobName);
            final var e = expectThrows(
                IOException.class,
                () -> blobContainer.writeBlob(
                    purpose,
                    blobName,
                    new BytesArray(randomBytes((int) ByteSizeValue.ofMb(10).getBytes())),
                    randomBoolean()
                )
            );
            final var cause = ExceptionsHelper.unwrap(e, ApiCallTimeoutException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), containsString("Client execution did not complete before the specified timeout configuration"));

            // TODO: AWS SDK classifies ApiCallTimeoutException as "Other" which seems to be a bug. Update the test once it is fixed.
            final var expectedErrorType = "Other";
            final Map<String, Object> expectedAttributes = Map.of(
                "repo_type",
                "s3",
                "repo_name",
                repository,
                "purpose",
                purpose.getKey(),
                "operation",
                Operation.PUT_OBJECT.getKey(),
                "error_type",
                expectedErrorType
            );
            {
                final var measurements = Measurement.combine(plugin.getLongCounterMeasurement(METRIC_EXCEPTIONS_TOTAL));
                assertThat(measurements, hasSize(1));
                assertThat(measurements.getFirst().attributes(), equalTo(expectedAttributes));
            }
            {
                final var measurements = plugin.getLongHistogramMeasurement(METRIC_EXCEPTIONS_HISTOGRAM);
                assertThat(measurements, hasSize(1));
                assertThat(measurements.getFirst().attributes(), equalTo(expectedAttributes));
            }
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void testRequestStats() throws Exception {
        // Skip testing request stats since the S3StallingHttpHandler does not track request stats
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an S3 endpoint")
    protected class S3StallingHttpHandler extends S3HttpHandler implements BlobStoreHttpHandler {

        private final AtomicReference<CountDownLatch> stallLatchRef = new AtomicReference<>(null);

        S3StallingHttpHandler(final String bucket) {
            super(bucket);
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final var latch = stallLatchRef.get();
            if (latch != null) {
                final String headerDecodedContentLength = exchange.getRequestHeaders().getFirst("x-amz-decoded-content-length");
                logger.info(
                    "--> Simulating server unresponsiveness for request [{} {}] with decoded content length [{}]",
                    exchange.getRequestMethod(),
                    exchange.getRequestURI(),
                    headerDecodedContentLength
                );
                safeAwait(latch, TimeValue.THIRTY_SECONDS);
                logger.info("--> Done simulating server unresponsiveness");
            }
            super.handle(exchange);
        }

        void setStallLatchRef(CountDownLatch latch) {
            stallLatchRef.set(latch);
        }
    }
}
