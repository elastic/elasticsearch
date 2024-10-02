/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.azure.AbstractAzureServerTestCase.randomBlobContent;
import static org.hamcrest.Matchers.greaterThan;

@SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
public class AzureBlobStoreRepositoryMetricsTests extends AzureBlobStoreRepositoryTests {

    private static final Predicate<HttpExchange> GET_BLOB_REQUEST_PREDICATE = request -> GET_BLOB_PATTERN.test(
        request.getRequestMethod() + " " + request.getRequestURI()
    );
    private static final int MAX_RETRIES = 3;

    private Queue<ErrorResponse> errorQueue = new ConcurrentLinkedQueue<>();

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        Map<String, HttpHandler> httpHandlers = super.createHttpHandlers();
        assert httpHandlers.size() == 1 : "This assumes there's a single handler";
        return httpHandlers.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ErrorInjectingAzureHttpHandler(errorQueue, e.getValue())));
    }

    /**
     * We want to control the errors in this test
     */
    @Override
    protected HttpHandler createErroneousHttpHandler(HttpHandler delegate) {
        return delegate;
    }

    @After
    public void checkErrorQueue() {
        if (errorQueue.isEmpty() == false) {
            fail("There were un-returned errors left in the queue, this is probably a broken test");
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
        IntStream.range(0, numThrottles).forEach(i -> errorQueue.offer(new ErrorResponse(RestStatus.TOO_MANY_REQUESTS)));

        // Check that the blob exists
        blobContainer.blobExists(purpose, blobName);

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES, repository).assertMetricsRecorded(
            numThrottles + 1,
            numThrottles,
            numThrottles,
            MetricsAsserter.Result.Success
        );
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
        errorQueue.offer(new ErrorResponse(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, null, GET_BLOB_REQUEST_PREDICATE));

        // Attempt to read the blob
        assertThrows(RequestedRangeNotSatisfiedException.class, () -> blobContainer.readBlob(purpose, blobName));

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB, repository).assertMetricsRecorded(
            1,
            0,
            1,
            MetricsAsserter.Result.RangeNotSatisfied
        );
    }

    public void testErrorResponsesAreCountedInMetrics() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Create a blob
        final String blobName = "index-" + randomIdentifier();
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);

        // Queue some retryable error responses
        final int numErrors = randomIntBetween(1, MAX_RETRIES);
        final AtomicInteger throttles = new AtomicInteger();
        IntStream.range(0, numErrors).forEach(i -> {
            RestStatus status = randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.TOO_MANY_REQUESTS, RestStatus.SERVICE_UNAVAILABLE);
            if (status == RestStatus.TOO_MANY_REQUESTS) {
                throttles.incrementAndGet();
            }
            errorQueue.offer(new ErrorResponse(status));
        });
        clearMetrics(dataNodeName);

        // Check that the blob exists
        blobContainer.blobExists(purpose, blobName);

        // Correct metrics are recorded
        metricsAsserter(dataNodeName, purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES, repository).assertMetricsRecorded(
            numErrors + 1,
            throttles.get(),
            numErrors,
            MetricsAsserter.Result.Success
        );
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
        }

        private MetricsAsserter(String dataNodeName, OperationPurpose purpose, AzureBlobStore.Operation operation, String repository) {
            this.dataNodeName = dataNodeName;
            this.purpose = purpose;
            this.operation = operation;
            this.repository = repository;
        }

        void assertMetricsRecorded(int expectedRequests, int expectedThrottles, int expectedExceptions, Result result) {
            assertCounterMetricRecorded(RepositoriesMetrics.METRIC_OPERATIONS_TOTAL, 1);
            assertCounterMetricRecorded(RepositoriesMetrics.METRIC_REQUESTS_TOTAL, expectedRequests);

            if (expectedThrottles > 0) {
                assertCounterMetricRecorded(RepositoriesMetrics.METRIC_THROTTLES_TOTAL, expectedThrottles);
                assertHistogramMetricRecorded(RepositoriesMetrics.METRIC_THROTTLES_HISTOGRAM, expectedThrottles);
            }

            if (expectedExceptions > 0) {
                assertCounterMetricRecorded(RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL, expectedExceptions);
                assertHistogramMetricRecorded(RepositoriesMetrics.METRIC_EXCEPTIONS_HISTOGRAM, expectedExceptions);
            }

            if (result == Result.RangeNotSatisfied || result == Result.Failure) {
                assertCounterMetricRecorded(RepositoriesMetrics.METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, 1);
            }

            if (result == Result.RangeNotSatisfied) {
                assertCounterMetricRecorded(RepositoriesMetrics.METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL, 1);
            }

            assertTimeToResponseMetricRecorded();
        }

        void assertCounterMetricRecorded(String metricName, int expectedValue) {
            assertIntValueMetricRecorded(getTelemetryPlugin(dataNodeName).getLongCounterMeasurement(metricName), expectedValue);
        }

        void assertHistogramMetricRecorded(String metricName, int expectedValue) {
            assertIntValueMetricRecorded(getTelemetryPlugin(dataNodeName).getLongHistogramMeasurement(metricName), expectedValue);
        }

        void assertTimeToResponseMetricRecorded() {
            assertMatchingMetricRecorded(
                getTelemetryPlugin(dataNodeName).getLongHistogramMeasurement(RepositoriesMetrics.HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM),
                m -> assertThat(m.getLong(), greaterThan(0L))
            );
        }

        private void assertIntValueMetricRecorded(List<Measurement> measurements, int expectedValue) {
            assertMatchingMetricRecorded(measurements, m -> assertEquals(expectedValue, m.getLong()));
        }

        private void assertMatchingMetricRecorded(List<Measurement> measurements, Consumer<Measurement> assertion) {
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
                        "No metric found with operation="
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
    private static class ErrorInjectingAzureHttpHandler implements DelegatingHttpHandler {

        private final HttpHandler delegate;
        private final Queue<ErrorResponse> errorResponseQueue;

        ErrorInjectingAzureHttpHandler(Queue<ErrorResponse> errorResponseQueue, HttpHandler delegate) {
            this.delegate = delegate;
            this.errorResponseQueue = errorResponseQueue;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            ErrorResponse nextError = errorResponseQueue.peek();
            if (nextError != null && nextError.matchesRequest(exchange)) {
                errorResponseQueue.poll().writeResponse(exchange);
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
    private static class ErrorResponse {

        private final RestStatus status;
        private final String responseBody;
        private final Predicate<HttpExchange> requestMatcher;

        ErrorResponse(RestStatus status) {
            this(status, null);
        }

        ErrorResponse(RestStatus status, String responseBody) {
            this(status, responseBody, req -> true);
        }

        /**
         * Create an error response that only gets returned for requests that match the supplied predicate. Note
         * that because the errors are stored in a queue this will prevent any subsequently queued errors from
         * being returned until after it returns.
         */
        ErrorResponse(RestStatus status, String responseBody, Predicate<HttpExchange> requestMatcher) {
            this.status = status;
            this.responseBody = responseBody;
            this.requestMatcher = requestMatcher;
        }

        public boolean matchesRequest(HttpExchange exchange) {
            return requestMatcher.test(exchange);
        }

        public void writeResponse(HttpExchange exchange) throws IOException {
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
