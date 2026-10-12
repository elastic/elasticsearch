/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prometheus.PrometheusPlugin;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteRequest;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteResponse;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrometheusRemoteWriteTransportActionTests extends ESTestCase {

    private PrometheusRemoteWriteTransportAction action;
    private Client client;
    private TransportService transportService;
    private ThreadPool threadPool;
    private Releasable indexingPressureRelease;
    private AtomicBoolean indexingPressureReleased;

    @Before
    public void initAction() {
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));
        transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.WRITE)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.absoluteTimeInMillis()).thenReturn(System.currentTimeMillis());

        action = new PrometheusRemoteWriteTransportAction(transportService, ActionFilters.EMPTY, threadPool, client, Settings.EMPTY);
    }

    @After
    public void assertIndexingPressureReleaseAfterTest() {
        if (indexingPressureRelease != null) {
            assertRegisteredIndexingPressureReleased("indexing pressure should be released after execution");
        }
    }

    public void testSuccess() {
        executeRequest(createWriteRequest("test_metric", 42.0, System.currentTimeMillis()));
    }

    public void testSuccessEmptyRequest() {
        executeRequest(createEmptyWriteRequest());
    }

    public void testSuccessWithMultipleTimeseries() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(createTimeSeries("metric_one", 1.0, now))
            .addTimeseries(createTimeSeries("metric_two", 2.0, now))
            .addTimeseries(createTimeSeries("metric_three_total", 3.0, now))
            .build();

        executeRequest(createWriteRequest(writeRequest, "generic", "default"));
    }

    public void testSuccessWithMultipleSamples() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(1.0).setTimestamp(now - 2000).build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(2.0).setTimestamp(now - 1000).build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(3.0).setTimestamp(now).build())
                    .build()
            )
            .build();

        executeRequest(createWriteRequest(writeRequest, "generic", "default"));
    }

    public void testSuccessWithExemplarOnly() throws Exception {
        assumeExemplarIngestionEnabled();
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("data_stream_dataset").setValue("custom").build())
                    .addExemplars(
                        RemoteWrite.Exemplar.newBuilder()
                            .addLabels(RemoteWrite.Label.newBuilder().setName("trace_id").setValue("abc123").build())
                            .setValue(21.0)
                            .setTimestamp(now)
                            .build()
                    )
                    .build()
            )
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");
        assertThat(bulk.numberOfActions(), equalTo(1));
        IndexRequest exemplarRequest = (IndexRequest) bulk.requests().getFirst();
        assertThat(exemplarRequest.index(), equalTo("exemplars-custom.prometheus-default"));

        ObjectPath source = new ObjectPath(exemplarRequest.sourceAsMap());
        assertThat(source.evaluate("@timestamp"), equalTo(now));
        assertThat(source.evaluate("data_stream.type"), equalTo("exemplars"));
        assertThat(source.evaluate("data_stream.dataset"), equalTo("custom.prometheus"));
        assertThat(source.evaluate("data_stream.namespace"), equalTo("default"));
        assertThat(source.evaluate("labels.__name__"), equalTo("test_metric"));
        assertThat(source.evaluate("labels.job"), equalTo("test"));
        assertNull(source.evaluate("labels.data_stream_dataset"));
        assertThat(source.evaluate("exemplar_labels.trace_id"), equalTo("abc123"));
        assertThat(source.evaluate("value"), equalTo(21.0));
    }

    public void testExemplarIngestionFollowsFeatureFlag() {
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createTimeSeries("test_metric", 42.0, timestamp).toBuilder()
                    .addExemplars(createExemplar("trace_id", "abc123", 21.0, timestamp))
                    .build()
            )
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        int expectedActions = PrometheusPlugin.METRIC_EXEMPLARS_FEATURE_FLAG.isEnabled() ? 2 : 1;
        assertThat(bulk.numberOfActions(), equalTo(expectedActions));
    }

    /**
     * Duplicate exemplars are not filtered up front; the time series index rejects them with a version conflict, which must be
     * treated as success.
     */
    public void testExemplarVersionConflictIsTreatedAsSuccess() {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createTimeSeries("test_metric", 42.0, timestamp).toBuilder()
                    .addExemplars(createExemplar("trace_id", "first", 1.0, timestamp))
                    .addExemplars(createExemplar("trace_id", "second", 2.0, timestamp))
                    .build()
            )
            .build();
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] {
                successResponse(),
                successResponse(),
                failureResponse("exemplars-generic.prometheus-default", RestStatus.CONFLICT, "version conflict") },
            0
        );

        executeRequest(createWriteRequest(writeRequest, "generic", "default"), listener -> listener.onResponse(bulkResponse));
    }

    public void testSameExemplarTimestampForDifferentSeriesIsRetained() {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(createExemplarTimeSeries("first_metric", timestamp))
            .addTimeseries(createExemplarTimeSeries("second_metric", timestamp))
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        assertThat(bulk.numberOfActions(), equalTo(2));
    }

    public void testMissingExemplarTimestampsUseRequestTimestamp() throws Exception {
        assumeExemplarIngestionEnabled();
        long requestTimestamp = randomLongBetween(1, Long.MAX_VALUE);
        when(threadPool.absoluteTimeInMillis()).thenReturn(requestTimestamp);
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(createExemplarTimeSeries("first_metric", 0))
            .addTimeseries(createExemplarTimeSeries("second_metric", 0))
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        assertThat(bulk.numberOfActions(), equalTo(2));
        for (DocWriteRequest<?> request : bulk.requests()) {
            ObjectPath source = new ObjectPath(((IndexRequest) request).sourceAsMap());
            assertThat(source.evaluate("@timestamp"), equalTo(requestTimestamp));
        }
        verify(threadPool).absoluteTimeInMillis();
    }

    /**
     * Exemplar failures never fail the request, regardless of their status (e.g. 400 for mapping issues, 403 when the API key
     * does not cover the exemplars data stream, or 429 which must not make the client re-send the samples).
     */
    public void testExemplarFailureDoesNotFailRequest() {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createTimeSeries("test_metric", 42.0, timestamp).toBuilder()
                    .addExemplars(createExemplar("trace_id", "abc123", 21.0, timestamp))
                    .build()
            )
            .build();
        RestStatus exemplarStatus = randomFrom(RestStatus.BAD_REQUEST, RestStatus.FORBIDDEN, RestStatus.TOO_MANY_REQUESTS);
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] {
                successResponse(),
                failureResponse("exemplars-generic.prometheus-default", exemplarStatus, "bad exemplar") },
            0
        );

        executeRequest(createWriteRequest(writeRequest, "generic", "default"), listener -> listener.onResponse(bulkResponse));
    }

    public void testExemplarFailureDoesNotAffectSampleFailureResponse() {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createTimeSeries("test_metric", 42.0, timestamp).toBuilder()
                    .addExemplars(createExemplar("trace_id", "abc123", 21.0, timestamp))
                    .build()
            )
            .build();
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] {
                failureResponse("metrics-generic.prometheus-default", RestStatus.BAD_REQUEST, "bad sample"),
                // a 429 on an exemplar must not turn the response into a 429
                failureResponse("exemplars-generic.prometheus-default", RestStatus.TOO_MANY_REQUESTS, "bad exemplar") },
            0
        );

        Exception e = executeRequestExpectingFailure(createWriteRequest(writeRequest, "generic", "default"), bulkResponse);

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("1 of 1 samples failed"));
        assertThat(e.getMessage(), containsString("bad sample"));
        // exemplar problems are only logged, never reported to the client
        assertThat(e.getMessage(), not(containsString("exemplar")));
    }

    public void testNonFiniteExemplarValuesAreDropped() throws Exception {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        double nonFinite = randomFrom(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createTimeSeries("test_metric", 42.0, timestamp).toBuilder()
                    .addExemplars(createExemplar("trace_id", "non_finite", nonFinite, timestamp))
                    .addExemplars(createExemplar("trace_id", "finite", 21.0, timestamp + 1))
                    .build()
            )
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        assertThat(bulk.numberOfActions(), equalTo(2));
        ObjectPath source = new ObjectPath(((IndexRequest) bulk.requests().get(1)).sourceAsMap());
        assertThat(source.evaluate("exemplar_labels.trace_id"), equalTo("finite"));
    }

    public void testExemplarDocumentsUseSameSeriesLabelsAsSamples() throws Exception {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    // Prometheus treats a label with an empty value as absent
                    .addLabels(RemoteWrite.Label.newBuilder().setName("empty").setValue("").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("data_stream_namespace").setValue("custom").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(timestamp).build())
                    .addExemplars(
                        RemoteWrite.Exemplar.newBuilder()
                            .addLabels(RemoteWrite.Label.newBuilder().setName("trace_id").setValue("abc123").build())
                            .addLabels(RemoteWrite.Label.newBuilder().setName("span_id").setValue("").build())
                            .setValue(21.0)
                            .setTimestamp(timestamp)
                            .build()
                    )
                    .build()
            )
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        assertThat(bulk.numberOfActions(), equalTo(2));
        ObjectPath sampleSource = new ObjectPath(((IndexRequest) bulk.requests().get(0)).sourceAsMap());
        ObjectPath exemplarSource = new ObjectPath(((IndexRequest) bulk.requests().get(1)).sourceAsMap());
        Map<String, Object> expectedLabels = Map.of("__name__", "test_metric", "job", "test");
        assertThat(sampleSource.evaluate("labels"), equalTo(expectedLabels));
        assertThat(exemplarSource.evaluate("labels"), equalTo(expectedLabels));
        assertThat(exemplarSource.evaluate("exemplar_labels"), equalTo(Map.of("trace_id", "abc123")));
    }

    public void testExemplarWithOnlyEmptyLabelsOmitsExemplarLabels() throws Exception {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                createExemplarTimeSeries("test_metric", timestamp).toBuilder()
                    .clearExemplars()
                    .addExemplars(createExemplar("trace_id", "", 21.0, timestamp))
                    .build()
            )
            .build();

        BulkRequest bulk = executeAndCaptureBulkRequest(writeRequest, "generic", "default");

        assertThat(bulk.numberOfActions(), equalTo(1));
        ObjectPath source = new ObjectPath(((IndexRequest) bulk.requests().getFirst()).sourceAsMap());
        assertNull(source.evaluate("exemplar_labels"));
        assertThat(source.evaluate("value"), equalTo(21.0));
    }

    public void test429() {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("metrics-generic.prometheus-default", RestStatus.TOO_MANY_REQUESTS, "too many requests"),
            successResponse() };

        Exception e = executeRequestExpectingFailure(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new BulkResponse(bulkItemResponses, 0)
        );

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testPartialSuccess() {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("metrics-generic.prometheus-default", RestStatus.BAD_REQUEST, "bad request"),
            successResponse() };

        Exception e = executeRequestExpectingFailure(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new BulkResponse(bulkItemResponses, 0)
        );

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("bad request"));
    }

    public void testBulkFailure() {
        Exception e = executeRequestExpectingFailure(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new IllegalStateException("bulk failure")
        );

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testInvalidProtobufReturns400() {
        RemoteWriteRequest request = createWriteRequest(new byte[] { 0x00, 0x01, 0x02, 0x03 }, "generic", "default");

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        assertThat(ExceptionsHelper.status(exception.getValue()), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testTimeseriesWithoutNameLabelReturns400() {
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                    .build()
            )
            .build();

        Exception e = executeRequestExpectingFailure(createWriteRequest(writeRequest, "generic", "default"));

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("missing __name__ label"));
    }

    public void testTimeseriesWithoutNameLabelReportsOnlySamples() {
        assumeExemplarIngestionEnabled();
        long timestamp = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(timestamp).build())
                    .addExemplars(createExemplar("trace_id", "abc123", 21.0, timestamp))
                    .build()
            )
            .build();

        Exception e = executeRequestExpectingFailure(createWriteRequest(writeRequest, "generic", "default"));

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("1 of 1 samples failed"));
        assertThat(e.getMessage(), containsString("1 sample(s) dropped due to missing __name__ label"));
        assertThat(e.getMessage(), not(containsString("exemplar")));
    }

    public void testExemplarOnlyTimeseriesWithoutNameLabelSucceeds() {
        assumeExemplarIngestionEnabled();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addExemplars(createExemplar("trace_id", "abc123", 21.0, System.currentTimeMillis()))
                    .build()
            )
            .build();

        executeRequest(createWriteRequest(writeRequest, "generic", "default"));
    }

    public void testPartialSuccessWithDroppedSamples() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(createTimeSeries("valid_metric", 1.0, now))
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(now).build())
                    .build()
            )
            .build();

        Exception e = executeRequestExpectingFailure(createWriteRequest(writeRequest, "generic", "default"));

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("missing __name__ label"));
    }

    public void testReleasesIndexingPressureAfterBulkExecute() {
        RemoteWriteRequest request = createWriteRequest("test_metric", 42.0, System.currentTimeMillis());

        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doAnswer(invocation -> {
            assertFalse("indexing pressure must not be released before bulk execute", indexingPressureReleased.get());
            return null;
        }).when(client).execute(any(), any(), bulkResponseListener.capture());

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, request, responseListener);

        assertRegisteredIndexingPressureReleased("indexing pressure should be released after bulk execute");
        bulkResponseListener.getValue().onResponse(new BulkResponse(new BulkItemResponse[] {}, 0));
    }

    public void testReleasesIndexingPressureOnInvalidProtobuf() {
        RemoteWriteRequest request = createWriteRequest(new byte[] { 0x00, 0x01, 0x02, 0x03 }, "generic", "default");

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, request, responseListener);

        assertRegisteredIndexingPressureReleased("indexing pressure should be released on invalid protobuf");
    }

    public void testReleasesIndexingPressureWhenExecutionShortCircuitsBeforeDoExecute() {
        RemoteWriteRequest request = createWriteRequest("test_metric", 42.0, System.currentTimeMillis());
        PrometheusRemoteWriteTransportAction shortCircuitingAction = new PrometheusRemoteWriteTransportAction(
            transportService,
            new ActionFilters(Set.of(new ActionFilter.Simple() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                protected boolean apply(String actionName, ActionRequest actionRequest, ActionListener<?> listener) {
                    listener.onFailure(new IllegalStateException("rejected before doExecute"));
                    return false;
                }
            })),
            threadPool,
            client,
            Settings.EMPTY
        );

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        shortCircuitingAction.execute(null, request, ActionListener.runBefore(responseListener, request::close));

        verify(responseListener).onFailure(any(Exception.class));
        verify(client, never()).prepareBulk();
        assertRegisteredIndexingPressureReleased("indexing pressure should be released when execution short-circuits");
    }

    public void testLabelFanoutReturns413() {
        long now = System.currentTimeMillis();
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        action = new PrometheusRemoteWriteTransportAction(transportService, ActionFilters.EMPTY, threadPool, client, settings);

        // ~1 KiB label value × enough samples that IndexRequest#ramBytesUsed() exceeds the 10 KiB limit
        String largeLabelValue = "x".repeat(1024);
        RemoteWrite.TimeSeries.Builder seriesBuilder = RemoteWrite.TimeSeries.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_metric").build())
            .addLabels(RemoteWrite.Label.newBuilder().setName("pad").setValue(largeLabelValue).build());
        for (int i = 0; i < 15; i++) {
            seriesBuilder.addSamples(RemoteWrite.Sample.newBuilder().setValue(i).setTimestamp(now + i).build());
        }

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().addTimeseries(seriesBuilder.build()).build();
        Exception e = executeRequestExpectingFailure(createWriteRequest(writeRequest, "generic", "default"));

        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(e.getMessage(), containsString("expanded content would exceed limit"));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testReleasesIndexingPressureOnLabelFanout() {
        long now = System.currentTimeMillis();
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        action = new PrometheusRemoteWriteTransportAction(transportService, ActionFilters.EMPTY, threadPool, client, settings);

        String largeLabelValue = "x".repeat(1024);
        RemoteWrite.TimeSeries.Builder seriesBuilder = RemoteWrite.TimeSeries.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("test_metric").build())
            .addLabels(RemoteWrite.Label.newBuilder().setName("pad").setValue(largeLabelValue).build());
        for (int i = 0; i < 15; i++) {
            seriesBuilder.addSamples(RemoteWrite.Sample.newBuilder().setValue(i).setTimestamp(now + i).build());
        }

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().addTimeseries(seriesBuilder.build()).build();
        RemoteWriteRequest request = createWriteRequest(writeRequest, "generic", "default");

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, request, responseListener);

        assertRegisteredIndexingPressureReleased("indexing pressure should be released on label fan-out rejection");
    }

    public void testStalenessMarkerIsDropped() {
        double stalenessMarker = Double.longBitsToDouble(0x7ff0000000000002L);
        executeRequest(createWriteRequest("stale_metric", stalenessMarker, System.currentTimeMillis()));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testNaNSamplesAreDropped() {
        executeRequest(createWriteRequest("nan_metric", Double.NaN, System.currentTimeMillis()));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testPositiveInfinitySamplesAreDropped() {
        executeRequest(createWriteRequest("inf_metric", Double.POSITIVE_INFINITY, System.currentTimeMillis()));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testNegativeInfinitySamplesAreDropped() {
        executeRequest(createWriteRequest("neg_inf_metric", Double.NEGATIVE_INFINITY, System.currentTimeMillis()));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testMixedFiniteAndNonFiniteSamples() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("mixed_metric").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(Double.NaN).setTimestamp(now - 2000).build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(now - 1000).build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(Double.POSITIVE_INFINITY).setTimestamp(now).build())
                    .build()
            )
            .build();

        executeRequest(createWriteRequest(writeRequest, "generic", "default"));
    }

    public void testCustomDatasetAndNamespace() {
        executeRequest(createWriteRequest("test_metric", 42.0, System.currentTimeMillis(), "myapp", "production"));
    }

    public void testDataStreamDatasetLabelCharactersAreSanitized() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue("metric_bad_ds_label").build())
                    .addLabels(RemoteWrite.Label.newBuilder().setName("data_stream_dataset").setValue("bad:name").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(1.0).setTimestamp(now).build())
                    .build()
            )
            .build();

        RemoteWriteRequest request = createWriteRequest(writeRequest, "generic", "default");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<BulkResponse>> bulkListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(client).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), bulkListenerCaptor.capture());

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, request, responseListener);

        bulkListenerCaptor.getValue().onResponse(new BulkResponse(new BulkItemResponse[] {}, 0));

        verify(responseListener).onResponse(any());
        BulkRequest bulk = bulkCaptor.getValue();
        assertThat(bulk.numberOfActions(), equalTo(1));
        assertThat(((IndexRequest) bulk.requests().get(0)).index(), equalTo("metrics-bad_name.prometheus-default"));
    }

    private void executeRequest(RemoteWriteRequest request) {
        executeRequest(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 0)));
    }

    private void executeRequest(RemoteWriteRequest request, Consumer<ActionListener<BulkResponse>> bulkResponseConsumer) {
        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        doExecuteRequest(request, bulkResponseConsumer, responseListener);
        verify(responseListener).onResponse(any());
    }

    private BulkRequest executeAndCaptureBulkRequest(RemoteWrite.WriteRequest writeRequest, String dataset, String namespace) {
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        ArgumentCaptor<ActionListener<BulkResponse>> bulkListenerCaptor = ArgumentCaptor.captor();
        doNothing().when(client).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), bulkListenerCaptor.capture());

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        action.doExecute(null, createWriteRequest(writeRequest, dataset, namespace), responseListener);
        bulkListenerCaptor.getValue().onResponse(new BulkResponse(new BulkItemResponse[] {}, 0));

        verify(responseListener).onResponse(any());
        return bulkCaptor.getValue();
    }

    private Exception executeRequestExpectingFailure(RemoteWriteRequest request) {
        return executeRequestExpectingFailure(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 0)));
    }

    private Exception executeRequestExpectingFailure(RemoteWriteRequest request, BulkResponse bulkResponse) {
        return executeRequestExpectingFailure(request, listener -> listener.onResponse(bulkResponse));
    }

    private Exception executeRequestExpectingFailure(RemoteWriteRequest request, Exception bulkFailure) {
        return executeRequestExpectingFailure(request, listener -> listener.onFailure(bulkFailure));
    }

    private Exception executeRequestExpectingFailure(
        RemoteWriteRequest request,
        Consumer<ActionListener<BulkResponse>> bulkResponseConsumer
    ) {
        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        doExecuteRequest(request, bulkResponseConsumer, responseListener);
        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        return exception.getValue();
    }

    private void doExecuteRequest(
        RemoteWriteRequest request,
        Consumer<ActionListener<BulkResponse>> bulkResponseConsumer,
        ActionListener<RemoteWriteResponse> responseListener
    ) {
        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), any(), bulkResponseListener.capture());

        action.doExecute(null, request, responseListener);

        if (bulkResponseListener.getAllValues().isEmpty() == false) {
            bulkResponseConsumer.accept(bulkResponseListener.getValue());
        }
    }

    private RemoteWriteRequest createEmptyWriteRequest() {
        return createWriteRequest(RemoteWrite.WriteRequest.newBuilder().build(), "generic", "default");
    }

    private RemoteWriteRequest createWriteRequest(String metricName, double value, long timestamp) {
        return createWriteRequest(metricName, value, timestamp, "generic", "default");
    }

    private RemoteWriteRequest createWriteRequest(String metricName, double value, long timestamp, String dataset, String ns) {
        return createWriteRequest(
            RemoteWrite.WriteRequest.newBuilder().addTimeseries(createTimeSeries(metricName, value, timestamp)).build(),
            dataset,
            ns
        );
    }

    private RemoteWriteRequest createWriteRequest(RemoteWrite.WriteRequest writeRequest, String dataset, String ns) {
        return createWriteRequest(writeRequest.toByteArray(), dataset, ns);
    }

    private RemoteWriteRequest createWriteRequest(byte[] payload, String dataset, String ns) {
        return new RemoteWriteRequest(
            ReleasableBytesReference.wrap(new BytesArray(payload)),
            dataset,
            ns,
            registerIndexingPressureRelease()
        );
    }

    private Releasable registerIndexingPressureRelease() {
        if (indexingPressureRelease != null) {
            assertRegisteredIndexingPressureReleased("indexing pressure should be released before registering a new releasable");
        }
        indexingPressureReleased = new AtomicBoolean(false);
        indexingPressureRelease = () -> indexingPressureReleased.set(true);
        return indexingPressureRelease;
    }

    private void assertRegisteredIndexingPressureReleased(String message) {
        assertNotNull("indexing pressure release state should be registered", indexingPressureReleased);
        assertTrue(message, indexingPressureReleased.get());
    }

    private static void assumeExemplarIngestionEnabled() {
        assumeTrue("requires metric exemplar ingestion", PrometheusPlugin.METRIC_EXEMPLARS_FEATURE_FLAG.isEnabled());
    }

    private static RemoteWrite.TimeSeries createTimeSeries(String metricName, double value, long timestamp) {
        return RemoteWrite.TimeSeries.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue(metricName).build())
            .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test_job").build())
            .addSamples(RemoteWrite.Sample.newBuilder().setValue(value).setTimestamp(timestamp).build())
            .build();
    }

    private static RemoteWrite.TimeSeries createExemplarTimeSeries(String metricName, long timestamp) {
        return RemoteWrite.TimeSeries.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue(metricName).build())
            .addExemplars(createExemplar("trace_id", "abc123", 21.0, timestamp))
            .build();
    }

    private static RemoteWrite.Exemplar createExemplar(String labelName, String labelValue, double value, long timestamp) {
        return RemoteWrite.Exemplar.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName(labelName).setValue(labelValue).build())
            .setValue(value)
            .setTimestamp(timestamp)
            .build();
    }

    private static BulkItemResponse successResponse() {
        return BulkItemResponse.success(-1, DocWriteRequest.OpType.CREATE, mock(DocWriteResponse.class));
    }

    private static BulkItemResponse failureResponse(String index, RestStatus restStatus, String failureMessage) {
        return BulkItemResponse.failure(
            -1,
            DocWriteRequest.OpType.CREATE,
            new BulkItemResponse.Failure(index, "id", new RuntimeException(failureMessage), restStatus)
        );
    }
}
