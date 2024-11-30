/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final Set<String> EXPECTED_MAIN_STORE_REQUEST_NAMES;
    private static final Set<String> EXPECTED_OBS_REQUEST_NAMES;
    private static final ByteSizeValue MULTIPART_UPLOAD_BUFFER_SIZE = ByteSizeValue.ofMb(5);

    static {
        final var mainStorePurposeNames = Set.of("ClusterState", "Indices", "Translog");
        final var obsPurposeNames = Set.of("SnapshotData", "SnapshotMetadata", "RepositoryAnalysis");
        final var operationNames = Set.of(
            "HeadObject",
            "GetObject",
            "ListObjects",
            "PutObject",
            "PutMultipartObject",
            "DeleteObjects",
            "AbortMultipartObject"
        );

        EXPECTED_MAIN_STORE_REQUEST_NAMES = Stream.concat(mainStorePurposeNames.stream(), obsPurposeNames.stream())
            .flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
            .collect(Collectors.toUnmodifiableSet());

        EXPECTED_OBS_REQUEST_NAMES = obsPurposeNames.stream()
            .flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
            .collect(Collectors.toUnmodifiableSet());
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private InterceptableS3HttpHandler s3HttpHandler;
    private int maxRetries;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(S3RepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        s3HttpHandler = new InterceptableS3HttpHandler("bucket");
        return Map.of("/bucket", s3HttpHandler);
    }

    @Before
    public void setupMaxRetries() {
        // max_retries set rarely to more than 1, so that we can test more retries within reasonable backoff
        maxRetries = rarely() ? randomIntBetween(2, 3) : 1;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("s3.client.test.access_key", "test_access_key");
        mockSecureSettings.setString("s3.client.test.secret_key", "test_secret_key");
        return super.nodeSettings().put("s3.client.test.endpoint", httpServerUrl())
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.S3)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .put("s3.client.test.max_retries", maxRetries)
            .setSecureSettings(mockSecureSettings);
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("bucket", "bucket")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        assertTrue(EXPECTED_MAIN_STORE_REQUEST_NAMES.containsAll(repositoryStats.requestCounts.keySet()));
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if (metricName.endsWith("_AbortMultipartObject") || metricName.endsWith("_HeadObject")) {
                assertThat(metricName, count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(metricName, count, greaterThan(0L));
            }
        });
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertTrue(EXPECTED_OBS_REQUEST_NAMES.containsAll(repositoryStats.requestCounts.keySet()));
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if (metricName.endsWith("_AbortMultipartObject") || metricName.endsWith("_PutMultipartObject")) {
                assertThat(count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(count, greaterThan(0L));
            }
        });
    }

    public void testShouldRetryMoreThanMaxRetriesForIndicesData() throws IOException {
        // More than the max retries by s3 client itself (1 original + maxRetries)
        final int errorsPerRequest = randomIntBetween(maxRetries + 1, maxRetries + 4);
        s3HttpHandler.setInterceptor(new Interceptor() {
            private final Map<String, AtomicInteger> requestPathErrorCount = ConcurrentCollections.newConcurrentMap();

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if (Regex.simpleMatch("GET /*/stateless_commit_*", request)) {
                    final AtomicInteger numberOfErrors = requestPathErrorCount.computeIfAbsent(request, k -> new AtomicInteger(0));
                    if (numberOfErrors.getAndIncrement() >= errorsPerRequest) {
                        return false;
                    }
                    try (exchange) {
                        ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                RestStatus.INTERNAL_SERVER_ERROR,
                                RestStatus.TOO_MANY_REQUESTS,
                                RestStatus.BAD_GATEWAY,
                                RestStatus.GATEWAY_TIMEOUT
                            ).getStatus(),
                            -1
                        );
                        return true;
                    }
                }
                return false;
            }
        });

        final Settings nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        final String loggerName = "org.elasticsearch.repositories.s3.S3RetryingInputStream";

        enum Status {
            STARTED, // On initial and subsequent failure messages before seeing the success message
            STOPPED, // On success message if failure messages have been observed before
            ERROR // Anything else
        }

        record CallSite(String threadName, String commitFile) {}

        final AtomicBoolean completed = new AtomicBoolean(false);
        try (var mockLogAppender = MockLog.capture(loggerName)) {
            // On each call site that experiences an initial failure, we expect to see a pairing of
            // failure messages (can be multiple) and the eventual success message.
            // We use a LoggingExpectation to perform bookkeeping for observed log messages and
            // ensures (1) we must see some related log messages and (2) the messages must make the pairing
            // (in the order of failure then success) for each call site.
            mockLogAppender.addExpectation(new MockLog.LoggingExpectation() {
                private final Pattern failurePattern = Pattern.compile(
                    "failed opening .*/(stateless_commit_[0-9]+).* at offset .* with purpose \\[Indices]; this was attempt .*"
                );
                private final Pattern eventualSuccessPattern = Pattern.compile(
                    "successfully open(?:ed)? input stream for .*/(stateless_commit_[0-9]+).* with purpose \\[Indices] after .* retries"
                );
                private final Map<CallSite, Status> bookkeeping = ConcurrentCollections.newConcurrentMap();

                @Override
                public void match(LogEvent event) {
                    if (event.getLevel().equals(Level.INFO) && event.getLoggerName().equals(loggerName)) {
                        if (maybeHandleAsFailureMessage(event)) {
                            return;
                        }
                        maybeHandleAsSuccessMessage(event);
                    }
                }

                @Override
                public void assertMatched() {
                    assertThat("Did not see any relevant logs", bookkeeping, not(anEmptyMap()));
                    assertThat("Seen log events: " + bookkeeping, bookkeeping.values(), everyItem(is(Status.STOPPED)));
                }

                private boolean maybeHandleAsFailureMessage(LogEvent event) {
                    final CallSite callSite = extractCallSite(event, failurePattern);
                    if (callSite == null) {
                        return false;
                    }
                    bookkeeping.compute(callSite, (k, v) -> {
                        if (v == null || v == Status.STARTED) {
                            return Status.STARTED;
                        } else {
                            return Status.ERROR;
                        }
                    });
                    return true;
                }

                private boolean maybeHandleAsSuccessMessage(LogEvent event) {
                    final CallSite callSite = extractCallSite(event, eventualSuccessPattern);
                    if (callSite == null) {
                        return false;
                    }
                    bookkeeping.compute(callSite, (k, v) -> {
                        if (v == Status.STARTED) {
                            return Status.STOPPED;
                        } else {
                            return Status.ERROR;
                        }
                    });
                    return true;
                }

                private CallSite extractCallSite(LogEvent event, Pattern pattern) {
                    final String formattedMessage = event.getMessage().getFormattedMessage();
                    final Matcher matcher = pattern.matcher(formattedMessage);
                    if (matcher.matches()) {
                        // The thread name including the node name and thread ID, e.g. elasticsearch[node_t0][refresh][T#2],
                        // which can be used to identify each call site and ensure the log messages match in pair,
                        // i.e. initial failure then eventually success
                        return new CallSite(event.getThreadName(), matcher.group(1));
                    } else {
                        return null;
                    }
                }
            });

            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
            ensureGreen(indexName);

            final CyclicBarrier uploadedCommitNotificationBarrier = new CyclicBarrier(2);
            MockTransportService.getInstance(searchNode)
                .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                    if (((NewCommitNotificationRequest) request).isUploaded()) {
                        handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                if (completed.get() == false) {
                                    safeAwait(uploadedCommitNotificationBarrier);
                                }
                                channel.sendResponse(response);
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                if (completed.get() == false) {
                                    fail(exception, "new uploaded commit notification should not fail");
                                }
                                // Exceptions can happen when the test ends which closes the index engine. They are innocuous
                                channel.sendResponse(exception);
                            }
                        }, task);
                    } else {
                        // Drop the notification for non-uploaded commits to force the search node reading from the object store
                        channel.sendResponse(new RuntimeException("dropping notification for non-uploaded commit"));
                    }
                });
            int totalNumDocs = 0;
            final int iterations = between(3, 5);
            for (int i = 0; i < iterations; i++) {
                final int numDocs = between(1, 10);
                totalNumDocs += numDocs;
                indexDocs(indexName, numDocs);
                indicesAdmin().prepareFlush(indexName).get(TimeValue.timeValueSeconds(10));
                safeAwait(uploadedCommitNotificationBarrier); // wait till uploaded commit notification is processed
                assertHitCount(client().prepareSearch(indexName), totalNumDocs); // ensure search works
            }
            logger.info("--> all search hit counts matched");
            mockLogAppender.assertAllExpectationsMatched();
            logger.info("--> logging expectation matched");
        } finally {
            completed.set(true);
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testShouldNotRetryForNoSuchFileException() throws Exception {
        final Settings nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        final String loggerName = "org.elasticsearch.repositories.s3.S3RetryingInputStream";

        try (var mockLogAppender = MockLog.capture(loggerName)) {
            mockLogAppender.addExpectation(
                new MockLog.UnseenEventExpectation("initial failure", loggerName, Level.INFO, "failed opening */stateless_commit_*")
            );
            mockLogAppender.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "final success",
                    loggerName,
                    Level.INFO,
                    "successfully opened input stream for */stateless_commit_* after * retries"
                )
            );

            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).build());
            ensureGreen(indexName);

            s3HttpHandler.setInterceptor(new Interceptor() {
                private final Set<String> erroredRequestPaths = ConcurrentCollections.newConcurrentSet();

                @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
                @Override
                public boolean intercept(HttpExchange exchange) throws IOException {
                    final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                    if (Regex.simpleMatch("GET /*/stateless_commit_*", request) && false == erroredRequestPaths.contains(request)) {
                        try (exchange) {
                            ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                            exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        }
                        return true;
                    }
                    return false;
                }
            });

            indexDocs(indexName, between(1, 10));
            final CountDownLatch newUploadedCommitNotificationFailureLatch = new CountDownLatch(1);
            MockTransportService.getInstance(searchNode)
                .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                    if (((NewCommitNotificationRequest) request).isUploaded()) {
                        handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                fail("new uploaded commit notification should have failed");
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                assertThat(ExceptionsHelper.unwrap(exception, NoSuchFileException.class), notNullValue());
                                newUploadedCommitNotificationFailureLatch.countDown();
                                channel.sendResponse(exception);
                            }
                        }, task);
                    } else {
                        // Drop the notification for non-uploaded commits to force the search node reading from the object store
                        channel.sendResponse(new RuntimeException("dropping notification for non-uploaded commit"));
                    }
                });
            indicesAdmin().prepareFlush(indexName).get(TimeValue.timeValueSeconds(10));
            // Search shard should fail because it encounters 404 while reading from the object store
            safeAwait(newUploadedCommitNotificationFailureLatch);
            mockLogAppender.assertAllExpectationsMatched();

        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testUploadIndicesDataWithRetries() throws Exception {
        s3HttpHandler.setInterceptor(new Interceptor() {
            private final int errorsPerRequest = randomIntBetween(3, 5);
            private final Map<String, AtomicInteger> requestPathErrorCount = ConcurrentCollections.newConcurrentMap();

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if (request.contains("PUT") && request.contains("indices/")) {
                    // request format: /bucket/base_path/indices/UUID/0/1/stateless_commit_N?x-purpose=Indices for single part uploads,
                    // while for multi-part uploads: .../stateless_commit_N?uploadId=UUID&partNumber=N&x-purpose=Indices where the
                    // upload UUID changes with every retry.
                    long gen = Long.parseLong(request.substring(request.lastIndexOf('_') + 1, request.lastIndexOf('?')));
                    long part = 1;
                    if (request.contains("partNumber")) {
                        part = Long.parseLong(
                            request.substring(request.indexOf("partNumber") + "partNumber=".length(), request.lastIndexOf('&'))
                        );
                    }
                    String genAndPart = gen + "/" + part;
                    final AtomicInteger numberOfErrors = requestPathErrorCount.computeIfAbsent(genAndPart, k -> new AtomicInteger(0));
                    if (numberOfErrors.getAndIncrement() >= errorsPerRequest) {
                        return false;
                    }
                    try (exchange) {
                        ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                RestStatus.INTERNAL_SERVER_ERROR,
                                RestStatus.TOO_MANY_REQUESTS,
                                RestStatus.BAD_GATEWAY,
                                RestStatus.GATEWAY_TIMEOUT
                            ).getStatus(),
                            -1
                        );
                        return true;
                    }
                }
                return false;
            }
        });

        final Settings nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        try {
            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 0).build());
            ensureGreen(indexName);
            IndexShard indexShard = findIndexShard(indexName);
            IndexEngine shardEngine = getShardEngine(indexShard, IndexEngine.class);

            // Index enough 1 MiB documents to produce a >5MiB Lucene compound segment file, to ensure S3 multi-part upload
            int numDocs = randomIntBetween(8, 10);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(indexName, "doc_" + i, "field", randomByteArrayOfLength((int) ByteSizeValue.ofMb(1).getBytes()));
            }

            flush(indexName);

            setReplicaCount(1, indexName);
            ensureGreen(indexName);
            assertHitCount(prepareSearch(indexName), numDocs);
        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testRetryOn403ForPutAndPost() throws IOException {
        final Set<String> requestsErroredFor403 = ConcurrentCollections.newConcurrentSet();
        s3HttpHandler.setInterceptor(new Interceptor() {
            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if ((request.startsWith("PUT /") || request.startsWith("POST /")) && requestsErroredFor403.add(request)) {
                    try (exchange) {
                        final byte[] response = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                              <Code>InvalidAccessKeyId</Code>
                              <Message>The AWS Access Key Id you provided does not exist in our records.</Message>
                              <RequestId>%s</RequestId>
                            </Error>""", randomUUID()).getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(403, response.length);
                        exchange.getResponseBody().write(response);
                    }
                    return true;
                }
                return false;
            }
        });

        final String masterAndIndexNode = startMasterAndIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(2);
        try {
            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).build());
            ensureGreen(indexName);
            final int batches = between(5, 20);
            for (int i = 0; i < batches; i++) {
                indexDocs(indexName, between(1, 10));
                refresh(indexName);
            }
            ensureGreen(indexName);
            // The test passes as long as the cluster forms successfully and completes some normal activities
            assertThat(requestsErroredFor403, not(empty()));
        } finally {
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testRetryOn403ForGet() throws Exception {
        final Set<String> requestsErroredFor403 = ConcurrentCollections.newConcurrentSet();
        var getBlob = new AtomicReference<String>();
        var listBlobs = new AtomicReference<String>();
        s3HttpHandler.setInterceptor(new Interceptor() {
            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if (getBlob.get() != null
                    && listBlobs.get() != null
                    && (request.startsWith(getBlob.get()) || request.startsWith(listBlobs.get()))
                    && requestsErroredFor403.add(request)) {
                    try (exchange) {
                        final byte[] response = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                              <Code>InvalidAccessKeyId</Code>
                              <Message>The AWS Access Key Id you provided does not exist in our records.</Message>
                              <RequestId>%s</RequestId>
                            </Error>""", randomUUID()).getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(403, response.length);
                        exchange.getResponseBody().write(response);
                    }
                    return true;
                }
                return false;
            }
        });

        final String masterNode = startMasterAndIndexNode();
        final String indexNode1 = startIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(3);

        String indexNode2 = null;
        try {
            var indexName = randomIdentifier();
            createIndex(
                indexName,
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode1).build()
            );
            var indexUuid = resolveIndex(indexName).getUUID();

            var basePath = getCurrentMasterObjectStoreService().getObjectStore().basePath().buildAsString().replaceAll("/", "%2F");
            listBlobs.set("GET /bucket/?prefix=" + basePath + "indices%2F" + indexUuid + "%2F");
            getBlob.set("GET /bucket/base_path/indices/" + indexUuid + "/0/");

            int iters = between(1, 5);
            for (int i = 0; i < iters; i++) {
                indexDocs(indexName, between(1, 10));
                flush(indexName);
                internalCluster().restartNode(indexNode1);
                ensureGreen(indexName);

                assertThat(requestsErroredFor403, not(empty()));
                requestsErroredFor403.clear();
            }

            indexNode2 = startIndexNode();
            ensureStableCluster(4);

            updateIndexSettings(
                Settings.builder().putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode2),
                indexName
            );
            ensureGreen(indexName);

            assertThat(requestsErroredFor403, not(empty()));
            requestsErroredFor403.clear();

            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", List.of(indexNode2, searchNode)),
                indexName
            );
            ensureGreen(indexName);
            // The test passes as long as the cluster forms successfully and completes some normal activities
            assertThat(requestsErroredFor403, not(empty()));
        } finally {
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(indexNode1);
            internalCluster().stopNode(indexNode2);
            internalCluster().stopNode(masterNode);
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class InterceptableS3HttpHandler extends S3HttpHandler {
        private volatile Interceptor interceptor;

        InterceptableS3HttpHandler(String bucket) {
            super(bucket);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (interceptor == null || false == interceptor.intercept(exchange)) {
                super.handle(exchange);
            }
        }

        public void setInterceptor(Interceptor interceptor) {
            this.interceptor = interceptor;
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    interface Interceptor {
        boolean intercept(HttpExchange exchange) throws IOException;
    }

    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected ObjectStoreService createObjectStoreService(
            Settings settings,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ClusterService clusterService
        ) {
            return new TestObjectStoreService(settings, repositoriesService, threadPool, clusterService);
        }
    }

    public static class TestObjectStoreService extends ObjectStoreService {

        public TestObjectStoreService(
            Settings settings,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ClusterService clusterService
        ) {
            super(settings, repositoriesService, threadPool, clusterService);
        }

        @Override
        protected Settings getRepositorySettings(ObjectStoreType type) {
            return Settings.builder().put(super.getRepositorySettings(type)).put("buffer_size", MULTIPART_UPLOAD_BUFFER_SIZE).build();
        }
    }
}
