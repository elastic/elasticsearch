/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.junit.After;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.azure.AbstractAzureServerTestCase.randomBlobContent;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
public class AzureBlobStoreRepositoryMetricsTests extends AzureBlobStoreRepositoryTests {

    private static final Predicate<HttpExchange> GET_BLOB_REQUEST_PREDICATE = request -> GET_BLOB_PATTERN.test(
        request.getRequestMethod() + " " + request.getRequestURI()
    );
    private static final int MAX_RETRIES = 3;

    private final Queue<RequestHandler> requestHandlers = new ConcurrentLinkedQueue<>();

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        Map<String, HttpHandler> httpHandlers = super.createHttpHandlers();
        assert httpHandlers.size() == 1 : "This assumes there's a single handler";
        return httpHandlers.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ResponseInjectingAzureHttpHandler(requestHandlers, e.getValue())));
    }

    /**
     * We want to control the errors in this test
     */
    @Override
    protected HttpHandler createErroneousHttpHandler(HttpHandler delegate) {
        return delegate;
    }

    @After
    public void checkRequestHandlerQueue() {
        if (requestHandlers.isEmpty() == false) {
            fail("There were unused request handlers left in the queue, this is probably a broken test");
        }
    }

    private static BlobContainer getBlobContainer(String dataNodeName, String repository) {
        final var blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(repository);
        return blobStoreRepository.blobStore().blobContainer(BlobPath.EMPTY.add(randomIdentifier()));
    }

    public void testThrottleResponsesAreCountedInMetrics() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Create a blob
        final String blobName = "index-" + randomIdentifier();
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
        clearMetrics(dataNodeName);

        // Queue up some throttle responses
        final int numThrottles = randomIntBetween(1, MAX_RETRIES);
        IntStream.range(0, numThrottles).forEach(i -> requestHandlers.offer(new FixedRequestHandler(RestStatus.TOO_MANY_REQUESTS)));

        // Check that the blob exists
        blobContainer.blobExists(purpose, blobName);

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES, repository).expectMetrics()
            .withRequests(numThrottles + 1)
            .withThrottles(numThrottles)
            .withExceptions(numThrottles)
            .forResult(MetricsAsserter.Result.Success);
    }

    public void testRangeNotSatisfiedAreCountedInMetrics() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Create a blob
        final String blobName = "index-" + randomIdentifier();
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
        clearMetrics(dataNodeName);

        // Queue up a range-not-satisfied error
        requestHandlers.offer(new FixedRequestHandler(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, null, GET_BLOB_REQUEST_PREDICATE));

        // Attempt to read the blob
        assertThrows(RequestedRangeNotSatisfiedException.class, () -> blobContainer.readBlob(purpose, blobName));

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB, repository).expectMetrics()
            .withRequests(1)
            .withThrottles(0)
            .withExceptions(1)
            .forResult(MetricsAsserter.Result.RangeNotSatisfied);
    }

    public void testErrorResponsesAreCountedInMetrics() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Create a blob
        final String blobName = "index-" + randomIdentifier();
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
        clearMetrics(dataNodeName);

        // Queue some retry-able error responses
        final int numErrors = randomIntBetween(1, MAX_RETRIES);
        final AtomicInteger throttles = new AtomicInteger();
        IntStream.range(0, numErrors).forEach(i -> {
            RestStatus status = randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.TOO_MANY_REQUESTS, RestStatus.SERVICE_UNAVAILABLE);
            if (status == RestStatus.TOO_MANY_REQUESTS) {
                throttles.incrementAndGet();
            }
            requestHandlers.offer(new FixedRequestHandler(status));
        });

        // Check that the blob exists
        blobContainer.blobExists(purpose, blobName);

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES, repository).expectMetrics()
            .withRequests(numErrors + 1)
            .withThrottles(throttles.get())
            .withExceptions(numErrors)
            .forResult(MetricsAsserter.Result.Success);
    }

    public void testRequestFailuresAreCountedInMetrics() {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);
        clearMetrics(dataNodeName);

        // Repeatedly cause a connection error to exhaust retries
        IntStream.range(0, MAX_RETRIES + 1).forEach(i -> requestHandlers.offer((exchange, delegate) -> exchange.close()));

        // Hit the API
        OperationPurpose purpose = randomFrom(OperationPurpose.values());
        assertThrows(IOException.class, () -> blobContainer.listBlobs(purpose));

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.LIST_BLOBS, repository).expectMetrics()
            .withRequests(4)
            .withThrottles(0)
            .withExceptions(4)
            .forResult(MetricsAsserter.Result.Exception);
    }

    public void testRequestTimeIsAccurate() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);
        clearMetrics(dataNodeName);

        AtomicLong totalDelayMillis = new AtomicLong(0);
        // Add some artificial delays
        IntStream.range(0, randomIntBetween(1, MAX_RETRIES)).forEach(i -> {
            long thisDelay = randomLongBetween(10, 100);
            totalDelayMillis.addAndGet(thisDelay);
            requestHandlers.offer((exchange, delegate) -> {
                safeSleep(thisDelay);
                // return a retry-able error
                exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            });
        });

        // Hit the API
        final long startTimeMillis = System.currentTimeMillis();
        blobContainer.listBlobs(randomFrom(OperationPurpose.values()));
        final long elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;

        List<Measurement> longHistogramMeasurement = getTelemetryPlugin(dataNodeName).getLongHistogramMeasurement(
            RepositoriesMetrics.HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM
        );
        long recordedRequestTime = longHistogramMeasurement.get(0).getLong();
        // Request time should be >= the delays we simulated
        assertThat(recordedRequestTime, greaterThanOrEqualTo(totalDelayMillis.get()));
        // And <= the elapsed time for the request
        assertThat(recordedRequestTime, lessThanOrEqualTo(elapsedTimeMillis));
    }

    public void testBatchDeleteFailure() throws IOException {
        final int deleteBatchSize = randomIntBetween(1, 30);
        final String repositoryName = randomRepositoryName();
        final String repository = createRepository(
            repositoryName,
            Settings.builder()
                .put(repositorySettings(repositoryName))
                .put(AzureRepository.Repository.DELETION_BATCH_SIZE_SETTING.getKey(), deleteBatchSize)
                .build(),
            true
        );
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer container = getBlobContainer(dataNodeName, repository);

        final List<String> blobsToDelete = new ArrayList<>();
        final int numberOfBatches = randomIntBetween(3, 20);
        final int numberOfBlobs = numberOfBatches * deleteBatchSize;
        final int failedBatches = randomIntBetween(1, numberOfBatches);
        for (int i = 0; i < numberOfBlobs; i++) {
            byte[] bytes = randomBytes(randomInt(100));
            String blobName = "index-" + randomAlphaOfLength(10);
            container.writeBlob(randomPurpose(), blobName, new BytesArray(bytes), false);
            blobsToDelete.add(blobName);
        }
        Randomness.shuffle(blobsToDelete);
        clearMetrics(dataNodeName);

        // Handler will fail one or more of the batch requests
        final RequestHandler failNRequestRequestHandler = createFailNRequestsHandler(failedBatches);

        // Exhaust the retries
        IntStream.range(0, (numberOfBatches - failedBatches) + (failedBatches * (MAX_RETRIES + 1)))
            .forEach(i -> requestHandlers.offer(failNRequestRequestHandler));

        logger.info("--> Failing {} of {} batches", failedBatches, numberOfBatches);

        final IOException exception = assertThrows(
            IOException.class,
            () -> container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobsToDelete.iterator())
        );
        assertEquals(Math.min(failedBatches, 10), exception.getSuppressed().length);
        assertEquals(
            (numberOfBatches - failedBatches) + (failedBatches * (MAX_RETRIES + 1L)),
            getLongCounterTotal(dataNodeName, RepositoriesMetrics.METRIC_REQUESTS_TOTAL)
        );
        assertEquals((failedBatches * (MAX_RETRIES + 1L)), getLongCounterTotal(dataNodeName, RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL));
        assertEquals(failedBatches * deleteBatchSize, container.listBlobs(randomPurpose()).size());
    }

    private long getLongCounterTotal(String dataNodeName, String metricKey) {
        return getTelemetryPlugin(dataNodeName).getLongCounterMeasurement(metricKey)
            .stream()
            .mapToLong(Measurement::getLong)
            .reduce(0L, Long::sum);
    }

    /**
     * Creates a {@link RequestHandler} that will persistently fail the first <code>numberToFail</code> distinct requests
     * it sees. Any other requests are passed through to the delegate.
     *
     * @param numberToFail The number of requests to fail
     * @return the handler
     */
    private static RequestHandler createFailNRequestsHandler(int numberToFail) {
        final List<String> requestsToFail = new ArrayList<>(numberToFail);
        return (exchange, delegate) -> {
            final Headers requestHeaders = exchange.getRequestHeaders();
            final String requestId = requestHeaders.get("X-ms-client-request-id").get(0);
            boolean failRequest = false;
            synchronized (requestsToFail) {
                if (requestsToFail.contains(requestId)) {
                    failRequest = true;
                } else if (requestsToFail.size() < numberToFail) {
                    requestsToFail.add(requestId);
                    failRequest = true;
                }
            }
            if (failRequest) {
                exchange.sendResponseHeaders(500, -1);
            } else {
                delegate.handle(exchange);
            }
        };
    }

    private void clearMetrics(String discoveryNode) {
        internalCluster().getInstance(PluginsService.class, discoveryNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .forEach(TestTelemetryPlugin::resetMeter);
    }

    private MetricsAsserter metricsAsserter(
        String dataNodeName,
        OperationPurpose operationPurpose,
        AzureBlobStore.Operation operation,
        String repository
    ) {
        return new MetricsAsserter(dataNodeName, operationPurpose, operation, repository);
    }

    private class MetricsAsserter {
        private final String dataNodeName;
        private final OperationPurpose purpose;
        private final AzureBlobStore.Operation operation;
        private final String repository;

        enum Result {
            Success,
            Failure,
            RangeNotSatisfied,
            Exception
        }

        enum MetricType {
            LongHistogram {
                @Override
                List<Measurement> getMeasurements(TestTelemetryPlugin testTelemetryPlugin, String name) {
                    return testTelemetryPlugin.getLongHistogramMeasurement(name);
                }
            },
            LongCounter {
                @Override
                List<Measurement> getMeasurements(TestTelemetryPlugin testTelemetryPlugin, String name) {
                    return testTelemetryPlugin.getLongCounterMeasurement(name);
                }
            };

            abstract List<Measurement> getMeasurements(TestTelemetryPlugin testTelemetryPlugin, String name);
        }

        private MetricsAsserter(String dataNodeName, OperationPurpose purpose, AzureBlobStore.Operation operation, String repository) {
            this.dataNodeName = dataNodeName;
            this.purpose = purpose;
            this.operation = operation;
            this.repository = repository;
        }

        private class Expectations {
            private int expectedRequests;
            private int expectedThrottles;
            private int expectedExceptions;

            public Expectations withRequests(int expectedRequests) {
                this.expectedRequests = expectedRequests;
                return this;
            }

            public Expectations withThrottles(int expectedThrottles) {
                this.expectedThrottles = expectedThrottles;
                return this;
            }

            public Expectations withExceptions(int expectedExceptions) {
                this.expectedExceptions = expectedExceptions;
                return this;
            }

            public void forResult(Result result) {
                assertMetricsRecorded(expectedRequests, expectedThrottles, expectedExceptions, result);
            }
        }

        Expectations expectMetrics() {
            return new Expectations();
        }

        private void assertMetricsRecorded(int expectedRequests, int expectedThrottles, int expectedExceptions, Result result) {
            assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_OPERATIONS_TOTAL, 1);
            assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_REQUESTS_TOTAL, expectedRequests);

            if (expectedThrottles > 0) {
                assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_THROTTLES_TOTAL, expectedThrottles);
                assertIntMetricRecorded(MetricType.LongHistogram, RepositoriesMetrics.METRIC_THROTTLES_HISTOGRAM, expectedThrottles);
            } else {
                assertNoMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_THROTTLES_TOTAL);
                assertNoMetricRecorded(MetricType.LongHistogram, RepositoriesMetrics.METRIC_THROTTLES_HISTOGRAM);
            }

            if (expectedExceptions > 0) {
                assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL, expectedExceptions);
                assertIntMetricRecorded(MetricType.LongHistogram, RepositoriesMetrics.METRIC_EXCEPTIONS_HISTOGRAM, expectedExceptions);
            } else {
                assertNoMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL);
                assertNoMetricRecorded(MetricType.LongHistogram, RepositoriesMetrics.METRIC_EXCEPTIONS_HISTOGRAM);
            }

            if (result == Result.RangeNotSatisfied || result == Result.Failure || result == Result.Exception) {
                assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, 1);
            } else {
                assertNoMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL);
            }

            if (result == Result.RangeNotSatisfied) {
                assertIntMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL, 1);
            } else {
                assertNoMetricRecorded(MetricType.LongCounter, RepositoriesMetrics.METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL);
            }

            assertMatchingMetricRecorded(
                MetricType.LongHistogram,
                RepositoriesMetrics.HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM,
                m -> assertThat("No request time metric found", m.getLong(), greaterThanOrEqualTo(0L))
            );
        }

        private void assertIntMetricRecorded(MetricType metricType, String metricName, int expectedValue) {
            assertMatchingMetricRecorded(
                metricType,
                metricName,
                measurement -> assertEquals("Unexpected value for " + metricType + " " + metricName, expectedValue, measurement.getLong())
            );
        }

        private void assertNoMetricRecorded(MetricType metricType, String metricName) {
            assertThat(
                "Expected no values for " + metricType + " " + metricName,
                metricType.getMeasurements(getTelemetryPlugin(dataNodeName), metricName),
                hasSize(0)
            );
        }

        private void assertMatchingMetricRecorded(MetricType metricType, String metricName, Consumer<Measurement> assertion) {
            List<Measurement> measurements = metricType.getMeasurements(getTelemetryPlugin(dataNodeName), metricName);
            Measurement measurement = measurements.stream()
                .filter(
                    m -> m.attributes().get("operation").equals(operation.getKey())
                        && m.attributes().get("purpose").equals(purpose.getKey())
                        && m.attributes().get("repo_name").equals(repository)
                        && m.attributes().get("repo_type").equals("azure")
                )
                .findFirst()
                .orElseThrow(
                    () -> new IllegalStateException(
                        "No metric found with name="
                            + metricName
                            + " and operation="
                            + operation.getKey()
                            + " and purpose="
                            + purpose.getKey()
                            + " and repo_name="
                            + repository
                            + " in "
                            + measurements
                    )
                );

            assertion.accept(measurement);
        }
    }

    @SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
    private static class ResponseInjectingAzureHttpHandler implements DelegatingHttpHandler {

        private final HttpHandler delegate;
        private final Queue<RequestHandler> requestHandlerQueue;

        ResponseInjectingAzureHttpHandler(Queue<RequestHandler> requestHandlerQueue, HttpHandler delegate) {
            this.delegate = delegate;
            this.requestHandlerQueue = requestHandlerQueue;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            RequestHandler nextHandler = requestHandlerQueue.peek();
            if (nextHandler != null && nextHandler.matchesRequest(exchange)) {
                requestHandlerQueue.poll().writeResponse(exchange, delegate);
            } else {
                delegate.handle(exchange);
            }
        }

        @Override
        public HttpHandler getDelegate() {
            return delegate;
        }
    }

    @SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
    @FunctionalInterface
    private interface RequestHandler {
        void writeResponse(HttpExchange exchange, HttpHandler delegate) throws IOException;

        default boolean matchesRequest(HttpExchange exchange) {
            return true;
        }
    }

    @SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
    private static class FixedRequestHandler implements RequestHandler {

        private final RestStatus status;
        private final String responseBody;
        private final Predicate<HttpExchange> requestMatcher;

        FixedRequestHandler(RestStatus status) {
            this(status, null, req -> true);
        }

        /**
         * Create a handler that only gets executed for requests that match the supplied predicate. Note
         * that because the errors are stored in a queue this will prevent any subsequently queued errors from
         * being returned until after it returns.
         */
        FixedRequestHandler(RestStatus status, String responseBody, Predicate<HttpExchange> requestMatcher) {
            this.status = status;
            this.responseBody = responseBody;
            this.requestMatcher = requestMatcher;
        }

        @Override
        public boolean matchesRequest(HttpExchange exchange) {
            return requestMatcher.test(exchange);
        }

        @Override
        public void writeResponse(HttpExchange exchange, HttpHandler delegateHandler) throws IOException {
            if (responseBody != null) {
                byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status.getStatus(), responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
            } else {
                exchange.sendResponseHeaders(status.getStatus(), -1);
            }
        }
    }
}
