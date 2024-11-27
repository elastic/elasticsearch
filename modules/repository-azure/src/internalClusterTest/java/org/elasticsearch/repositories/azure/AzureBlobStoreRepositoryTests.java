/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.BackgroundIndexer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_OPERATIONS_TOTAL;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    protected static final String DEFAULT_ACCOUNT_NAME = "account";
    protected static final Predicate<String> LIST_PATTERN = Pattern.compile("GET /[a-zA-Z0-9]+/[a-zA-Z0-9]+\\?.+").asMatchPredicate();
    protected static final Predicate<String> GET_BLOB_PATTERN = Pattern.compile("GET /[a-zA-Z0-9]+/[a-zA-Z0-9]+/.+").asMatchPredicate();

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
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .put(AzureRepository.Repository.DELETION_BATCH_SIZE_SETTING.getKey(), randomIntBetween(5, 256))
            .put(AzureRepository.Repository.MAX_CONCURRENT_BATCH_DELETES_SETTING.getKey(), randomIntBetween(1, 10));
        if (randomBoolean()) {
            settingsBuilder.put(AzureRepository.Repository.BASE_PATH_SETTING.getKey(), randomFrom("test", "test/1"));
        }
        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestAzureRepositoryPlugin.class, TestTelemetryPlugin.class);
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
        return new AzureHTTPStatsCollectorHandler(new AzureErroneousHttpHandler(delegate, AzureStorageSettings.DEFAULT_MAX_RETRIES));
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

    protected TestTelemetryPlugin getTelemetryPlugin(String dataNodeName) {
        return internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
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
                        5L,
                        10L,
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
            super(account, container, null /* no auth header validation - sometimes it's omitted in these tests (TODO why?) */);
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
        private final Set<String> seenRequestIds = ConcurrentCollections.newConcurrentSet();

        private AzureHTTPStatsCollectorHandler(HttpHandler delegate) {
            super(delegate);
        }

        @Override
        protected void maybeTrack(String request, Headers headers) {
            // Same request id is a retry
            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-ncnbi/817da997-30d2-4cd3-972f-a0073e4e98f7
            // Do not count retries since the client side request stats do not track them yet.
            // See https://github.com/elastic/elasticsearch/issues/104443
            if (false == seenRequestIds.add(headers.getFirst("X-ms-client-request-id"))) {
                return;
            }
            if (GET_BLOB_PATTERN.test(request)) {
                trackRequest("GetBlob");
            } else if (Regex.simpleMatch("HEAD /*/*/*", request)) {
                trackRequest("GetBlobProperties");
            } else if (LIST_PATTERN.test(request)) {
                trackRequest("ListBlobs");
            } else if (isPutBlock(request)) {
                trackRequest("PutBlock");
            } else if (isPutBlockList(request)) {
                trackRequest("PutBlockList");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutBlob");
            } else if (Regex.simpleMatch("POST /*/*?*comp=batch*", request)) {
                trackRequest("BlobBatch");
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
                container.writeBlob(randomPurpose(), blobName, new BytesArray(bytes), false);
            }

            container.delete(randomPurpose());
            assertThat(container.listBlobs(randomPurpose()), is(anEmptyMap()));
        }
    }

    public void testDeleteBlobsIgnoringIfNotExists() throws Exception {
        // Test with a smaller batch size here
        final int deleteBatchSize = randomIntBetween(1, 30);
        final String repositoryName = randomRepositoryName();
        createRepository(
            repositoryName,
            Settings.builder()
                .put(repositorySettings(repositoryName))
                .put(AzureRepository.Repository.DELETION_BATCH_SIZE_SETTING.getKey(), deleteBatchSize)
                .build(),
            true
        );
        try (BlobStore store = newBlobStore(repositoryName)) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            final int toDeleteCount = randomIntBetween(deleteBatchSize, 3 * deleteBatchSize);
            final List<String> blobsToDelete = new ArrayList<>();
            for (int i = 0; i < toDeleteCount; i++) {
                byte[] bytes = randomBytes(randomInt(100));
                String blobName = randomAlphaOfLength(10);
                container.writeBlob(randomPurpose(), blobName, new BytesArray(bytes), false);
                blobsToDelete.add(blobName);
            }

            // Try to delete non existent blobs
            for (int i = 0; i < 10; i++) {
                blobsToDelete.add(randomName());
            }

            Randomness.shuffle(blobsToDelete);
            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobsToDelete.iterator());
            assertThat(container.listBlobs(randomPurpose()), is(anEmptyMap()));
        }
    }

    public void testNotFoundErrorMessageContainsFullKey() throws Exception {
        try (BlobStore store = newBlobStore()) {
            BlobContainer container = store.blobContainer(BlobPath.EMPTY.add("nested").add("dir"));
            NoSuchFileException exception = expectThrows(NoSuchFileException.class, () -> container.readBlob(randomPurpose(), "blob"));
            assertThat(exception.getMessage(), containsString("nested/dir/blob] not found"));
        }
    }

    public void testReadByteByByte() throws Exception {
        try (BlobStore store = newBlobStore()) {
            BlobContainer container = store.blobContainer(BlobPath.EMPTY.add(UUIDs.randomBase64UUID()));
            var data = randomBytes(randomIntBetween(128, 512));
            String blobName = randomName();
            container.writeBlob(randomPurpose(), blobName, new ByteArrayInputStream(data), data.length, true);

            var originalDataInputStream = new ByteArrayInputStream(data);
            try (var azureInputStream = container.readBlob(randomPurpose(), blobName)) {
                for (int i = 0; i < data.length; i++) {
                    assertThat(originalDataInputStream.read(), is(equalTo(azureInputStream.read())));
                }

                assertThat(azureInputStream.read(), is(equalTo(-1)));
                assertThat(originalDataInputStream.read(), is(equalTo(-1)));
            }
            container.delete(randomPurpose());
        }
    }

    public void testMetrics() throws Exception {
        // Reset all the metrics so there's none lingering from previous tests
        internalCluster().getInstances(PluginsService.class)
            .forEach(ps -> ps.filterPlugins(TestTelemetryPlugin.class).forEach(TestTelemetryPlugin::resetMeter));

        // Create the repository and perform some activities
        final String repository = createRepository(randomRepositoryName(), false);
        final String index = "index-no-merges";
        createIndex(index, 1, 0);

        final long nbDocs = randomLongBetween(10_000L, 20_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), (int) nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }
        flushAndRefresh(index);
        BroadcastResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(prepareSearch(index).setSize(0).setTrackTotalHits(true), nbDocs);

        final String snapshot = "snapshot";
        assertSuccessfulSnapshot(
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot).setWaitForCompletion(true).setIndices(index)
        );
        assertAcked(client().admin().indices().prepareDelete(index));
        assertSuccessfulRestore(
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot).setWaitForCompletion(true)
        );
        ensureGreen(index);
        assertHitCount(prepareSearch(index).setSize(0).setTrackTotalHits(true), nbDocs);
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot).get());

        final Map<AzureBlobStore.Operation, Long> aggregatedMetrics = new HashMap<>();
        // Compare collected stats and metrics for each node and they should be the same
        for (var nodeName : internalCluster().getNodeNames()) {
            final BlobStoreRepository blobStoreRepository;
            try {
                blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, nodeName)
                    .repository(repository);
            } catch (RepositoryMissingException e) {
                continue;
            }

            final AzureBlobStore blobStore = (AzureBlobStore) blobStoreRepository.blobStore();
            final Map<AzureBlobStore.StatsKey, LongAdder> statsCollectors = blobStore.getMetricsRecorder().opsCounters;

            final List<Measurement> metrics = Measurement.combine(
                getTelemetryPlugin(nodeName).getLongCounterMeasurement(METRIC_OPERATIONS_TOTAL)
            );

            assertThat(
                statsCollectors.keySet().stream().map(AzureBlobStore.StatsKey::operation).collect(Collectors.toSet()),
                equalTo(
                    metrics.stream()
                        .map(m -> AzureBlobStore.Operation.fromKey((String) m.attributes().get("operation")))
                        .collect(Collectors.toSet())
                )
            );
            metrics.forEach(metric -> {
                assertThat(
                    metric.attributes(),
                    allOf(hasEntry("repo_type", AzureRepository.TYPE), hasKey("repo_name"), hasKey("operation"), hasKey("purpose"))
                );
                final AzureBlobStore.Operation operation = AzureBlobStore.Operation.fromKey((String) metric.attributes().get("operation"));
                final AzureBlobStore.StatsKey statsKey = new AzureBlobStore.StatsKey(
                    operation,
                    OperationPurpose.parse((String) metric.attributes().get("purpose"))
                );
                assertThat(nodeName + "/" + statsKey + " exists", statsCollectors, hasKey(statsKey));
                assertThat(nodeName + "/" + statsKey + " has correct sum", metric.getLong(), equalTo(statsCollectors.get(statsKey).sum()));
                aggregatedMetrics.compute(statsKey.operation(), (k, v) -> v == null ? metric.getLong() : v + metric.getLong());
            });
        }

        // Metrics number should be consistent with server side request count as well.
        assertThat(aggregatedMetrics, equalTo(getServerMetrics()));
    }

    private Map<AzureBlobStore.Operation, Long> getServerMetrics() {
        return getMockRequestCounts().entrySet()
            .stream()
            .collect(Collectors.toMap(e -> AzureBlobStore.Operation.fromKey(e.getKey()), Map.Entry::getValue));
    }
}
