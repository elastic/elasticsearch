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

import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_REQUESTS_TOTAL;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomNonDataPurpose;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
// Need to set up a new cluster for each test because cluster settings use randomized authentication settings
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class S3BlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    private static final TimeValue TEST_COOLDOWN_PERIOD = TimeValue.timeValueSeconds(10L);

    private String region;
    private String signerOverride;
    private final AtomicBoolean shouldFailCompleteMultipartUploadRequest = new AtomicBoolean();

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
        shouldFailCompleteMultipartUploadRequest.set(false);
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
        return List.of(TestS3RepositoryPlugin.class, TestTelemetryPlugin.class);
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
    @TestIssueLogging(issueUrl = "https://github.com/elastic/elasticsearch/issues/88841", value = "com.amazonaws.request:DEBUG")
    public void testRequestStats() throws Exception {
        super.testRequestStats();
    }

    public void testAbortRequestStats() throws Exception {
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

        // Intentionally fail snapshot to trigger abortMultipartUpload requests
        shouldFailCompleteMultipartUploadRequest.set(true);
        final String snapshot = "snapshot";
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot).setWaitForCompletion(true).setIndices(index).get();
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot).get();

        final RepositoryStats repositoryStats = StreamSupport.stream(
            internalCluster().getInstances(RepositoriesService.class).spliterator(),
            false
        ).map(repositoriesService -> {
            try {
                return repositoriesService.repository(repository);
            } catch (RepositoryMissingException e) {
                return null;
            }
        }).filter(Objects::nonNull).map(Repository::stats).reduce(RepositoryStats::merge).get();

        Map<String, Long> sdkRequestCounts = repositoryStats.requestCounts;
        assertThat(sdkRequestCounts.get("AbortMultipartObject"), greaterThan(0L));
        assertThat(sdkRequestCounts.get("DeleteObjects"), greaterThan(0L));

        final Map<String, Long> mockCalls = getMockRequestCounts();

        String assertionErrorMsg = String.format("SDK sent [%s] calls and handler measured [%s] calls", sdkRequestCounts, mockCalls);
        assertEquals(assertionErrorMsg, mockCalls, sdkRequestCounts);
    }

    @TestIssueLogging(issueUrl = "https://github.com/elastic/elasticsearch/issues/101608", value = "com.amazonaws.request:DEBUG")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/101608")
    public void testMetrics() throws Exception {
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

        final Map<S3BlobStore.StatsKey, Long> aggregatedMetrics = new HashMap<>();
        // Compare collected stats and metrics for each node and they should be the same
        for (var nodeName : internalCluster().getNodeNames()) {
            final BlobStoreRepository blobStoreRepository;
            try {
                blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, nodeName)
                    .repository(repository);
            } catch (RepositoryMissingException e) {
                continue;
            }

            final BlobStore blobStore = blobStoreRepository.blobStore();
            final BlobStore delegateBlobStore = ((BlobStoreWrapper) blobStore).delegate();
            final S3BlobStore s3BlobStore = (S3BlobStore) delegateBlobStore;
            final Map<S3BlobStore.StatsKey, S3BlobStore.IgnoreNoResponseMetricsCollector> statsCollectors = s3BlobStore
                .getStatsCollectors().collectors;

            final var plugins = internalCluster().getInstance(PluginsService.class, nodeName)
                .filterPlugins(TestTelemetryPlugin.class)
                .toList();
            assertThat(plugins, hasSize(1));
            final List<Measurement> metrics = Measurement.combine(plugins.get(0).getLongCounterMeasurement(METRIC_REQUESTS_TOTAL));

            assertThat(
                statsCollectors.keySet().stream().map(S3BlobStore.StatsKey::operation).collect(Collectors.toSet()),
                equalTo(
                    metrics.stream()
                        .map(m -> S3BlobStore.Operation.parse((String) m.attributes().get("operation")))
                        .collect(Collectors.toSet())
                )
            );
            metrics.forEach(metric -> {
                assertThat(
                    metric.attributes(),
                    allOf(hasEntry("repo_type", S3Repository.TYPE), hasKey("repo_name"), hasKey("operation"), hasKey("purpose"))
                );
                final S3BlobStore.Operation operation = S3BlobStore.Operation.parse((String) metric.attributes().get("operation"));
                final S3BlobStore.StatsKey statsKey = new S3BlobStore.StatsKey(
                    operation,
                    OperationPurpose.parse((String) metric.attributes().get("purpose"))
                );
                assertThat(nodeName + "/" + statsKey + " exists", statsCollectors, hasKey(statsKey));
                assertThat(
                    nodeName + "/" + statsKey + " has correct sum",
                    metric.getLong(),
                    equalTo(statsCollectors.get(statsKey).counter.sum())
                );
                aggregatedMetrics.compute(statsKey, (k, v) -> v == null ? metric.getLong() : v + metric.getLong());
            });
        }

        // Metrics number should be consistent with server side request count as well.
        assertThat(aggregatedMetrics, equalTo(getServerMetrics()));
    }

    public void testRequestStatsWithOperationPurposes() throws IOException {
        final String repoName = createRepository(randomRepositoryName());
        final RepositoriesService repositoriesService = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        final BlobStoreWrapper blobStore = asInstanceOf(BlobStoreWrapper.class, repository.blobStore());
        final S3BlobStore delegateBlobStore = asInstanceOf(S3BlobStore.class, blobStore.delegate());
        final S3BlobStore.StatsCollectors statsCollectors = delegateBlobStore.getStatsCollectors();

        // Initial stats are collected for repository verification, which counts as SNAPSHOT_METADATA
        final Set<String> allOperations = EnumSet.allOf(S3BlobStore.Operation.class)
            .stream()
            .map(S3BlobStore.Operation::getKey)
            .collect(Collectors.toUnmodifiableSet());
        assertThat(
            statsCollectors.collectors.keySet().stream().map(S3BlobStore.StatsKey::purpose).collect(Collectors.toUnmodifiableSet()),
            equalTo(Set.of(OperationPurpose.SNAPSHOT_METADATA))
        );
        final Map<String, Long> initialStats = blobStore.stats();
        assertThat(initialStats.keySet(), equalTo(allOperations));

        // Collect more stats with an operation purpose other than the default
        final OperationPurpose purpose = randomValueOtherThan(OperationPurpose.SNAPSHOT_METADATA, BlobStoreTestUtil::randomPurpose);
        final BlobPath blobPath = repository.basePath().add(randomAlphaOfLength(10));
        final BlobContainer blobContainer = blobStore.blobContainer(blobPath);
        final BytesArray whatToWrite = new BytesArray(randomByteArrayOfLength(randomIntBetween(100, 1000)));
        blobContainer.writeBlob(purpose, "test.txt", whatToWrite, true);
        try (InputStream is = blobContainer.readBlob(purpose, "test.txt")) {
            is.readAllBytes();
        }
        blobContainer.delete(purpose);

        // Internal stats collection is fine-grained and records different purposes
        assertThat(
            statsCollectors.collectors.keySet().stream().map(S3BlobStore.StatsKey::purpose).collect(Collectors.toUnmodifiableSet()),
            equalTo(Set.of(OperationPurpose.SNAPSHOT_METADATA, purpose))
        );
        // The stats report aggregates over different purposes
        final Map<String, Long> newStats = blobStore.stats();
        assertThat(newStats.keySet(), equalTo(allOperations));
        assertThat(newStats, not(equalTo(initialStats)));

        // Exercise stats report that keep find grained information
        final Map<String, Long> fineStats = statsCollectors.statsMap(true);
        assertThat(
            fineStats.keySet(),
            equalTo(
                statsCollectors.collectors.keySet().stream().map(S3BlobStore.StatsKey::toString).collect(Collectors.toUnmodifiableSet())
            )
        );
        // fine stats are equal to coarse grained stats (without entries with value 0) by aggregation
        assertThat(
            fineStats.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().split("_", 2)[1], Collectors.summingLong(Map.Entry::getValue))),
            equalTo(
                newStats.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != 0L)
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
            )
        );

        final Set<String> operationsSeenForTheNewPurpose = statsCollectors.collectors.keySet()
            .stream()
            .filter(sk -> sk.purpose() != OperationPurpose.SNAPSHOT_METADATA)
            .map(sk -> sk.operation().getKey())
            .collect(Collectors.toUnmodifiableSet());

        newStats.forEach((k, v) -> {
            if (operationsSeenForTheNewPurpose.contains(k)) {
                assertThat(newStats.get(k), greaterThan(initialStats.get(k)));
            } else {
                assertThat(newStats.get(k), equalTo(initialStats.get(k)));
            }
        });
    }

    public void testEnforcedCooldownPeriod() throws IOException {
        final String repoName = randomRepositoryName();
        createRepository(
            repoName,
            Settings.builder().put(repositorySettings(repoName)).put(S3Repository.COOLDOWN_PERIOD.getKey(), TEST_COOLDOWN_PERIOD).build(),
            true
        );

        final SnapshotId fakeOldSnapshot = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-old")
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
                        IndexVersion.fromId(6080099),   // minimum node version compatible with 7.6.0
                        0L, // -1 would refresh RepositoryData and find the real version
                        0L, // -1 would refresh RepositoryData and find the real version,
                        "" // null would refresh RepositoryData and find the real version
                    )
                )
            );
        final BytesReference serialized = BytesReference.bytes(
            modifiedRepositoryData.snapshotsToXContent(XContentFactory.jsonBuilder(), SnapshotsService.OLD_SNAPSHOT_FORMAT)
        );
        repository.blobStore()
            .blobContainer(repository.basePath())
            .writeBlobAtomic(
                randomNonDataPurpose(),
                BlobStoreRepository.INDEX_FILE_PREFIX + modifiedRepositoryData.getGenId(),
                serialized,
                true
            );

        final String newSnapshotName = "snapshot-new";
        final long beforeThrottledSnapshot = repository.threadPool().relativeTimeInNanos();
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, newSnapshotName).setWaitForCompletion(true).setIndices().get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeThrottledSnapshot, greaterThan(TEST_COOLDOWN_PERIOD.getNanos()));

        final long beforeThrottledDelete = repository.threadPool().relativeTimeInNanos();
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, newSnapshotName).get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeThrottledDelete, greaterThan(TEST_COOLDOWN_PERIOD.getNanos()));

        final long beforeFastDelete = repository.threadPool().relativeTimeInNanos();
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, fakeOldSnapshot.getName()).get();
        assertThat(repository.threadPool().relativeTimeInNanos() - beforeFastDelete, lessThan(TEST_COOLDOWN_PERIOD.getNanos()));
    }

    private Map<S3BlobStore.StatsKey, Long> getServerMetrics() {
        for (HttpHandler h : handlers.values()) {
            while (h instanceof DelegatingHttpHandler) {
                if (h instanceof S3StatsCollectorHttpHandler s3StatsCollectorHttpHandler) {
                    return Maps.transformValues(s3StatsCollectorHttpHandler.getMetricsCount(), AtomicLong::get);
                }
                h = ((DelegatingHttpHandler) h).getDelegate();
            }
        }
        return Collections.emptyMap();
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
            RecoverySettings recoverySettings,
            S3RepositoriesMetrics s3RepositoriesMetrics
        ) {
            return new S3Repository(metadata, registry, getService(), clusterService, bigArrays, recoverySettings, s3RepositoriesMetrics) {

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
    protected class S3BlobStoreHttpHandler extends S3HttpHandler implements BlobStoreHttpHandler {

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
    protected static class S3ErroneousHttpHandler extends ErroneousHttpHandler {

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
    protected class S3StatsCollectorHttpHandler extends HttpStatsCollectorHandler {

        private final Map<S3BlobStore.StatsKey, AtomicLong> metricsCount = ConcurrentCollections.newConcurrentMap();

        S3StatsCollectorHttpHandler(final HttpHandler delegate) {
            super(delegate);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final S3HttpHandler.RequestComponents requestComponents = S3HttpHandler.parseRequestComponents(
                S3HttpHandler.getRawRequestString(exchange)
            );
            if (false == requestComponents.request().startsWith("HEAD ")) {
                assertThat(requestComponents.customQueryParameters(), hasKey(S3BlobStore.CUSTOM_QUERY_PARAMETER_PURPOSE));
            }
            final String request = requestComponents.request();
            if (shouldFailCompleteMultipartUploadRequest.get() && Regex.simpleMatch("POST /*/*?uploadId=*", request)) {
                try (exchange) {
                    drainInputStream(exchange.getRequestBody());
                    exchange.sendResponseHeaders(
                        randomFrom(RestStatus.BAD_REQUEST.getStatus(), RestStatus.TOO_MANY_REQUESTS.getStatus()),
                        -1
                    );
                    return;
                }
            }
            super.handle(exchange);
        }

        @Override
        public void maybeTrack(final String rawRequest, Headers requestHeaders) {
            final S3HttpHandler.RequestComponents requestComponents = S3HttpHandler.parseRequestComponents(rawRequest);
            final String request = requestComponents.request();
            final OperationPurpose purpose = OperationPurpose.parse(
                requestComponents.customQueryParameters().get(S3BlobStore.CUSTOM_QUERY_PARAMETER_PURPOSE).get(0)
            );
            if (Regex.simpleMatch("GET /*/?prefix=*", request)) {
                trackRequest("ListObjects");
                metricsCount.computeIfAbsent(new S3BlobStore.StatsKey(S3BlobStore.Operation.LIST_OBJECTS, purpose), k -> new AtomicLong())
                    .incrementAndGet();
            } else if (Regex.simpleMatch("GET /*/*", request)) {
                trackRequest("GetObject");
                metricsCount.computeIfAbsent(new S3BlobStore.StatsKey(S3BlobStore.Operation.GET_OBJECT, purpose), k -> new AtomicLong())
                    .incrementAndGet();
            } else if (isMultiPartUpload(request)) {
                trackRequest("PutMultipartObject");
                metricsCount.computeIfAbsent(
                    new S3BlobStore.StatsKey(S3BlobStore.Operation.PUT_MULTIPART_OBJECT, purpose),
                    k -> new AtomicLong()
                ).incrementAndGet();
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutObject");
                metricsCount.computeIfAbsent(new S3BlobStore.StatsKey(S3BlobStore.Operation.PUT_OBJECT, purpose), k -> new AtomicLong())
                    .incrementAndGet();
            } else if (Regex.simpleMatch("POST /*/?delete", request)) {
                trackRequest("DeleteObjects");
                metricsCount.computeIfAbsent(new S3BlobStore.StatsKey(S3BlobStore.Operation.DELETE_OBJECTS, purpose), k -> new AtomicLong())
                    .incrementAndGet();
            } else if (Regex.simpleMatch("DELETE /*/*?uploadId=*", request)) {
                trackRequest("AbortMultipartObject");
                metricsCount.computeIfAbsent(
                    new S3BlobStore.StatsKey(S3BlobStore.Operation.ABORT_MULTIPART_OBJECT, purpose),
                    k -> new AtomicLong()
                ).incrementAndGet();
            } else if (Regex.simpleMatch("HEAD /*/*", request)) {
                trackRequest("HeadObject");
                metricsCount.computeIfAbsent(new S3BlobStore.StatsKey(S3BlobStore.Operation.HEAD_OBJECT, purpose), k -> new AtomicLong())
                    .incrementAndGet();
            } else {
                logger.info("--> rawRequest not tracked [{}] with parsed purpose [{}]", request, purpose.getKey());
            }
        }

        Map<S3BlobStore.StatsKey, AtomicLong> getMetricsCount() {
            return metricsCount;
        }

        private boolean isMultiPartUpload(String request) {
            return Regex.simpleMatch("POST /*/*?uploads", request)
                || Regex.simpleMatch("POST /*/*?*uploadId=*", request)
                || Regex.simpleMatch("PUT /*/*?*uploadId=*", request);
        }
    }
}
