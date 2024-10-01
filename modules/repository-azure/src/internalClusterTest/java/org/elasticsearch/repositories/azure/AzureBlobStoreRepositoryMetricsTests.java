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
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.azure.AbstractAzureServerTestCase.randomBlobContent;

@SuppressForbidden(reason = "we use a HttpServer to emulate Azure")
public class AzureBlobStoreRepositoryMetricsTests extends AzureBlobStoreRepositoryTests {

    private Queue<ErrorResponse> errorQueue = new ConcurrentLinkedQueue<>();

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        Map<String, HttpHandler> httpHandlers = super.createHttpHandlers();
        assert httpHandlers.size() == 1 : "This only works if there's a single handler";
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

    private static BlobContainer getBlobContainer(String dataNodeName, String repository) {
        final var blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(repository);
        return blobStoreRepository.blobStore().blobContainer(BlobPath.EMPTY.add(randomIdentifier()));
    }

    public void testRetriesAreCountedInMetrics() throws IOException {
        int numThrottles = randomIntBetween(1, 2);
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Queue up some throttle responses
        IntStream.range(0, numThrottles).forEach(i -> errorQueue.offer(new ErrorResponse(RestStatus.TOO_MANY_REQUESTS)));

        // Create a blob
        String blobName = "index-" + randomIdentifier();
        OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);

        TestTelemetryPlugin plugin = getTelemetryPlugin(dataNodeName);
        List<Measurement> throttlesHistogram = plugin.getLongHistogramMeasurement(RepositoriesMetrics.METRIC_THROTTLES_HISTOGRAM);
        assertEquals(numThrottles, throttlesHistogram.get(0).value().intValue());
        List<Measurement> throttlesTotal = plugin.getLongCounterMeasurement(RepositoriesMetrics.METRIC_THROTTLES_TOTAL);
        assertEquals(numThrottles, throttlesTotal.get(0).value().intValue());
    }

    public void testRangeNotSatisfiedAreCountedInMetrics() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);

        // Create a blob
        String blobName = "index-" + randomIdentifier();
        OperationPurpose purpose = randomFrom(OperationPurpose.values());
        blobContainer.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);

        // Queue up a range-not-satisfied error
        errorQueue.offer(new ErrorResponse(RestStatus.REQUESTED_RANGE_NOT_SATISFIED));

        // Get the blob
        assertThrows(RequestedRangeNotSatisfiedException.class, () -> blobContainer.readBlob(purpose, blobName));

        TestTelemetryPlugin plugin = getTelemetryPlugin(dataNodeName);
        List<Measurement> rangeNotSatisfiedCounter = plugin.getLongCounterMeasurement(
            RepositoriesMetrics.METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL
        );
        assertEquals(1, rangeNotSatisfiedCounter.get(0).value().intValue());
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
            if (errorResponseQueue.isEmpty()) {
                delegate.handle(exchange);
            } else {
                errorResponseQueue.poll().writeResponse(exchange);
            }
        }

        @Override
        public HttpHandler getDelegate() {
            return delegate;
        }
    }

    private static class ErrorResponse {

        private final RestStatus status;
        private final String responseBody;

        ErrorResponse(RestStatus status) {
            this(status, null);
        }

        ErrorResponse(RestStatus status, String responseBody) {
            this.status = status;
            this.responseBody = responseBody;
        }

        @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
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
