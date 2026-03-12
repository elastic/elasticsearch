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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteRequest;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteResponse;
import org.junit.After;
import org.mockito.ArgumentCaptor;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));
        transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.WRITE)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        action = new PrometheusRemoteWriteTransportAction(transportService, new ActionFilters(Set.of()), threadPool, client);
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
            client
        );

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);
        shortCircuitingAction.execute(null, request, ActionListener.runBefore(responseListener, request::close));

        verify(responseListener).onFailure(any(Exception.class));
        verify(client, never()).prepareBulk();
        assertRegisteredIndexingPressureReleased("indexing pressure should be released when execution short-circuits");
    }

    public void testCustomDatasetAndNamespace() {
        executeRequest(createWriteRequest("test_metric", 42.0, System.currentTimeMillis(), "myapp", "production"));
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

    private static RemoteWrite.TimeSeries createTimeSeries(String metricName, double value, long timestamp) {
        return RemoteWrite.TimeSeries.newBuilder()
            .addLabels(RemoteWrite.Label.newBuilder().setName("__name__").setValue(metricName).build())
            .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test_job").build())
            .addSamples(RemoteWrite.Sample.newBuilder().setValue(value).setTimestamp(timestamp).build())
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
