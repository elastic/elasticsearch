/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.repositories.s3.S3BlobStore.Operation;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.repositories.RepositoriesMetrics.HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_EXCEPTIONS_HISTOGRAM;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_OPERATIONS_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_REQUESTS_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_THROTTLES_HISTOGRAM;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_THROTTLES_TOTAL;
import static org.elasticsearch.repositories.RepositoriesMetrics.METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.REQUESTED_RANGE_NOT_SATISFIED;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
// Need to set up a new cluster for each test because cluster settings use randomized authentication settings
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class S3BlobStoreRepositoryMetricsTests extends S3BlobStoreRepositoryTests {

    private final Queue<RestStatus> errorStatusQueue = new LinkedBlockingQueue<>();

    // Always create erroneous handler
    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap(
            "/bucket",
            new S3StatsCollectorHttpHandler(new S3MetricErroneousHttpHandler(new S3BlobStoreHttpHandler("bucket"), errorStatusQueue))
        );
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return delegate;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings settings = super.nodeSettings(nodeOrdinal, otherSettings);
        return Settings.builder()
            .put(settings)
            .put(S3ClientSettings.MAX_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), 4)
            .build();
    }

    private static TestTelemetryPlugin getPlugin(String dataNodeName) {
        var plugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();
        return plugin;
    }

    private static BlobContainer getBlobContainer(String dataNodeName, String repository) {
        final var blobStoreRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(repository);
        return blobStoreRepository.blobStore().blobContainer(BlobPath.EMPTY.add(randomIdentifier()));
    }

    public void testHttpRequestTimeCaptureInMilliseconds() throws IOException {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final TestTelemetryPlugin plugin = getPlugin(dataNodeName);
        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);
        final String blobName = randomIdentifier();

        long before = System.nanoTime();
        blobContainer.writeBlob(purpose, blobName, new BytesArray(randomBytes(between(10, 1000))), false);
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before);
        assertThat(getLongHistogramValue(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.PUT_OBJECT), lessThanOrEqualTo(elapsed));

        plugin.resetMeter();
        before = System.nanoTime();
        blobContainer.readBlob(purpose, blobName).close();
        elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before);
        assertThat(getLongHistogramValue(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.GET_OBJECT), lessThanOrEqualTo(elapsed));

        plugin.resetMeter();
        before = System.nanoTime();
        blobContainer.deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
        elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before);
        assertThat(
            getLongHistogramValue(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.DELETE_OBJECTS),
            lessThanOrEqualTo(elapsed)
        );
    }

    public void testMetricsWithErrors() throws IOException {
        final String repository = createRepository(randomRepositoryName());

        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final TestTelemetryPlugin plugin = getPlugin(dataNodeName);

        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);
        final String blobName = randomIdentifier();

        // Put a blob
        final int nPuts = randomIntBetween(1, 3);
        for (int i = 0; i < nPuts; i++) {
            final long batch = i + 1;
            addErrorStatus(INTERNAL_SERVER_ERROR, TOO_MANY_REQUESTS, TOO_MANY_REQUESTS);
            blobContainer.writeBlob(purpose, blobName, new BytesArray("blob"), false);
            assertThat(getLongCounterValue(plugin, METRIC_REQUESTS_TOTAL, Operation.PUT_OBJECT), equalTo(4L * batch));
            assertThat(getLongCounterValue(plugin, METRIC_OPERATIONS_TOTAL, Operation.PUT_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, Operation.PUT_OBJECT), equalTo(0L));
            assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_TOTAL, Operation.PUT_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_THROTTLES_TOTAL, Operation.PUT_OBJECT), equalTo(2L * batch));
            assertThat(getLongHistogramValue(plugin, METRIC_EXCEPTIONS_HISTOGRAM, Operation.PUT_OBJECT), equalTo(batch));
            assertThat(getLongHistogramValue(plugin, METRIC_THROTTLES_HISTOGRAM, Operation.PUT_OBJECT), equalTo(2L * batch));
            assertThat(getNumberOfMeasurements(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.PUT_OBJECT), equalTo(batch));
        }

        // Get not found
        final int nGets = randomIntBetween(1, 3);
        for (int i = 0; i < nGets; i++) {
            final long batch = i + 1;
            addErrorStatus(TOO_MANY_REQUESTS, NOT_FOUND);
            try {
                blobContainer.readBlob(purpose, blobName).close();
            } catch (Exception e) {
                // intentional failure
            }
            assertThat(getLongCounterValue(plugin, METRIC_REQUESTS_TOTAL, Operation.GET_OBJECT), equalTo(2L * batch));
            assertThat(getLongCounterValue(plugin, METRIC_OPERATIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_THROTTLES_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongHistogramValue(plugin, METRIC_EXCEPTIONS_HISTOGRAM, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongHistogramValue(plugin, METRIC_THROTTLES_HISTOGRAM, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getNumberOfMeasurements(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.GET_OBJECT), equalTo(batch));

            // Make sure we don't hit the request range not satisfied counters
            assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL, Operation.GET_OBJECT), equalTo(0L));
        }

        // List retry exhausted
        final int nLists = randomIntBetween(1, 3);
        for (int i = 0; i < nLists; i++) {
            final long batch = i + 1;
            addErrorStatus(TOO_MANY_REQUESTS, TOO_MANY_REQUESTS, TOO_MANY_REQUESTS, TOO_MANY_REQUESTS, TOO_MANY_REQUESTS);
            try {
                blobContainer.listBlobs(purpose);
            } catch (Exception e) {
                // intentional failure
            }
            assertThat(getLongCounterValue(plugin, METRIC_REQUESTS_TOTAL, Operation.LIST_OBJECTS), equalTo(5L * batch));
            assertThat(getLongCounterValue(plugin, METRIC_OPERATIONS_TOTAL, Operation.LIST_OBJECTS), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, Operation.LIST_OBJECTS), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_TOTAL, Operation.LIST_OBJECTS), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_THROTTLES_TOTAL, Operation.LIST_OBJECTS), equalTo(5L * batch));
            assertThat(getLongHistogramValue(plugin, METRIC_EXCEPTIONS_HISTOGRAM, Operation.LIST_OBJECTS), equalTo(batch));
            assertThat(getLongHistogramValue(plugin, METRIC_THROTTLES_HISTOGRAM, Operation.LIST_OBJECTS), equalTo(5L * batch));
            assertThat(getNumberOfMeasurements(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.LIST_OBJECTS), equalTo(batch));
        }

        // Delete to clean up
        blobContainer.deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
        assertThat(getLongCounterValue(plugin, METRIC_REQUESTS_TOTAL, Operation.DELETE_OBJECTS), equalTo(1L));
        assertThat(getLongCounterValue(plugin, METRIC_OPERATIONS_TOTAL, Operation.DELETE_OBJECTS), equalTo(1L));
        assertThat(getLongCounterValue(plugin, METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, Operation.DELETE_OBJECTS), equalTo(0L));
        assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_TOTAL, Operation.DELETE_OBJECTS), equalTo(0L));
        assertThat(getLongCounterValue(plugin, METRIC_THROTTLES_TOTAL, Operation.DELETE_OBJECTS), equalTo(0L));
        assertThat(getLongHistogramValue(plugin, METRIC_EXCEPTIONS_HISTOGRAM, Operation.DELETE_OBJECTS), equalTo(0L));
        assertThat(getLongHistogramValue(plugin, METRIC_THROTTLES_HISTOGRAM, Operation.DELETE_OBJECTS), equalTo(0L));
        assertThat(getNumberOfMeasurements(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.DELETE_OBJECTS), equalTo(1L));
    }

    public void testMetricsForRequestRangeNotSatisfied() {
        final String repository = createRepository(randomRepositoryName());
        final String dataNodeName = internalCluster().getNodeNameThat(DiscoveryNode::canContainData);
        final BlobContainer blobContainer = getBlobContainer(dataNodeName, repository);
        final TestTelemetryPlugin plugin = getPlugin(dataNodeName);

        final OperationPurpose purpose = randomFrom(OperationPurpose.values());
        final String blobName = randomIdentifier();

        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            final long batch = i + 1;
            addErrorStatus(TOO_MANY_REQUESTS, TOO_MANY_REQUESTS, REQUESTED_RANGE_NOT_SATISFIED);
            try {
                blobContainer.readBlob(purpose, blobName).close();
            } catch (Exception e) {
                assertThat(e, instanceOf(RequestedRangeNotSatisfiedException.class));
            }

            assertThat(getLongCounterValue(plugin, METRIC_REQUESTS_TOTAL, Operation.GET_OBJECT), equalTo(3 * batch));
            assertThat(getLongCounterValue(plugin, METRIC_OPERATIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongCounterValue(plugin, METRIC_EXCEPTIONS_TOTAL, Operation.GET_OBJECT), equalTo(batch));
            assertThat(getLongHistogramValue(plugin, METRIC_EXCEPTIONS_HISTOGRAM, Operation.GET_OBJECT), equalTo(batch));
            assertThat(
                getLongCounterValue(plugin, METRIC_EXCEPTIONS_REQUEST_RANGE_NOT_SATISFIED_TOTAL, Operation.GET_OBJECT),
                equalTo(batch)
            );
            assertThat(getLongCounterValue(plugin, METRIC_THROTTLES_TOTAL, Operation.GET_OBJECT), equalTo(2 * batch));
            assertThat(getLongHistogramValue(plugin, METRIC_THROTTLES_HISTOGRAM, Operation.GET_OBJECT), equalTo(2 * batch));
            assertThat(getNumberOfMeasurements(plugin, HTTP_REQUEST_TIME_IN_MILLIS_HISTOGRAM, Operation.GET_OBJECT), equalTo(batch));
        }
    }

    private void addErrorStatus(RestStatus... statuses) {
        errorStatusQueue.addAll(Arrays.asList(statuses));
    }

    private long getLongCounterValue(TestTelemetryPlugin plugin, String instrumentName, Operation operation) {
        final List<Measurement> measurements = Measurement.combine(plugin.getLongCounterMeasurement(instrumentName));
        return measurements.stream()
            .filter(m -> m.attributes().get("operation") == operation.getKey())
            .mapToLong(Measurement::getLong)
            .findFirst()
            .orElse(0L);
    }

    private long getNumberOfMeasurements(TestTelemetryPlugin plugin, String instrumentName, Operation operation) {
        final List<Measurement> measurements = plugin.getLongHistogramMeasurement(instrumentName);
        return measurements.stream().filter(m -> m.attributes().get("operation") == operation.getKey()).count();
    }

    private long getLongHistogramValue(TestTelemetryPlugin plugin, String instrumentName, Operation operation) {
        final List<Measurement> measurements = Measurement.combine(plugin.getLongHistogramMeasurement(instrumentName));
        return measurements.stream()
            .filter(m -> m.attributes().get("operation") == operation.getKey())
            .mapToLong(Measurement::getLong)
            .findFirst()
            .orElse(0L);
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class S3MetricErroneousHttpHandler implements DelegatingHttpHandler {

        private final HttpHandler delegate;
        private final Queue<RestStatus> errorStatusQueue;

        S3MetricErroneousHttpHandler(HttpHandler delegate, Queue<RestStatus> errorStatusQueue) {
            this.delegate = delegate;
            this.errorStatusQueue = errorStatusQueue;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final RestStatus status = errorStatusQueue.poll();
            if (status == null) {
                delegate.handle(exchange);
            } else if (status == INTERNAL_SERVER_ERROR) {
                // Simulate an retryable exception
                throw new IOException("ouch");
            } else {
                try (exchange) {
                    drainInputStream(exchange.getRequestBody());
                    exchange.sendResponseHeaders(status.getStatus(), -1);
                }
            }
        }

        public HttpHandler getDelegate() {
            return delegate;
        }
    }
}
