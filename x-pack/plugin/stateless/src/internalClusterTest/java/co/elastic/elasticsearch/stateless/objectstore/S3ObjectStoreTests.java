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

import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.MockLogAppender;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final Set<String> EXPECTED_REQUEST_NAMES = Set.of(
        "HeadObject",
        "GetObject",
        "ListObjects",
        "PutObject",
        "PutMultipartObject",
        "DeleteObjects",
        "AbortMultipartObject"
    );

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private InterceptableS3HttpHandler s3HttpHandler;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(S3RepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        s3HttpHandler = new InterceptableS3HttpHandler("bucket");
        return Map.of("/bucket", s3HttpHandler);
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
            .put("s3.client.test.max_retries", 1) // default to 1 so that we can test more retries within reasonable backoff
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/pull/1543")
    @Override
    public void testBlobStoreStats() throws IOException {
        super.testBlobStoreStats();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if ("AbortMultipartObject".equals(metricName) || "HeadObject".equals(metricName)) {
                assertThat(metricName, count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(metricName, count, greaterThan(0L));
            }
        });
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if ("AbortMultipartObject".equals(metricName) || "PutMultipartObject".equals(metricName)) {
                assertThat(count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(count, greaterThan(0L));
            }
        });
    }

    public void testShouldRetryMoreThanMaxRetriesForIndicesData() throws IOException {
        s3HttpHandler.setInterceptor(new Interceptor() {
            private final int errorsPerRequest = randomIntBetween(3, 5); // More than the default attempts of 2 (1 original + 1 retry)
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

        final String masterAndIndexNode = startMasterAndIndexNode();
        final String searchNode = startSearchNode();

        final var mockLogAppender = new MockLogAppender();
        final String loggerName = "org.elasticsearch.repositories.s3.S3RetryingInputStream";

        enum Status {
            STARTED, // On initial and subsequent failure messages before seeing the success message
            STOPPED, // On success message if failure messages have been observed before
            ERROR // Anything else
        }

        record CallSite(String threadName, String commitFile) {}

        try (var ignored = mockLogAppender.capturing(loggerName)) {
            // On each call site that experiences an initial failure, we expect to see a pairing of
            // failure messages (can be multiple) and the eventual success message.
            // We use a LoggingExpectation to perform bookkeeping for observed log messages and
            // ensures (1) we must see some related log messages and (2) the messages must make the pairing
            // (in the order of failure then success) for each call site.
            mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
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
            createIndex(indexName, indexSettings(1, 1).build());
            ensureGreen(indexName);

            for (int i = 0; i < randomIntBetween(3, 8); i++) {
                indexDocs(indexName, randomIntBetween(1, 10));
                refresh(indexName);
            }

            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testShouldNotRetryForNoSuchFileException() throws IOException {
        final String masterAndIndexNode = startMasterAndIndexNode();
        final String searchNode = startSearchNode();

        final var mockLogAppender = new MockLogAppender();
        final String loggerName = "org.elasticsearch.repositories.s3.S3RetryingInputStream";

        try (var ignored = mockLogAppender.capturing(loggerName)) {
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("initial failure", loggerName, Level.INFO, "failed opening */stateless_commit_*")
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
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

            indexDocs(indexName, randomIntBetween(1, 10));

            final BroadcastResponse broadcastResponse = indicesAdmin().prepareRefresh(indexName).get(TimeValue.timeValueSeconds(10));
            assertThat(broadcastResponse.getFailedShards(), greaterThan(0));
            assertThat(
                Arrays.stream(broadcastResponse.getShardFailures()).map(DefaultShardOperationFailedException::status).toList(),
                everyItem(oneOf(RestStatus.NOT_FOUND, RestStatus.INTERNAL_SERVER_ERROR))
            );
            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class InterceptableS3HttpHandler extends S3HttpHandler {
        private Interceptor interceptor;

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
}
