/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteRequest;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction.RemoteWriteResponse;
import org.mockito.ArgumentCaptor;

import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrometheusRemoteWriteTransportActionTests extends ESTestCase {

    private PrometheusRemoteWriteTransportAction action;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));

        action = new PrometheusRemoteWriteTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client
        );
    }

    public void testSuccess() {
        RemoteWriteResponse response = executeRequest(createWriteRequest("test_metric", 42.0, System.currentTimeMillis()));

        assertThat(response.getStatus(), equalTo(RestStatus.NO_CONTENT));
    }

    public void testSuccessEmptyRequest() {
        RemoteWriteResponse response = executeRequest(createEmptyWriteRequest());

        assertThat(response.getStatus(), equalTo(RestStatus.NO_CONTENT));
    }

    public void testSuccessWithMultipleTimeseries() {
        long now = System.currentTimeMillis();
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(createTimeSeries("metric_one", 1.0, now))
            .addTimeseries(createTimeSeries("metric_two", 2.0, now))
            .addTimeseries(createTimeSeries("metric_three_total", 3.0, now))
            .build();

        RemoteWriteResponse response = executeRequest(new RemoteWriteRequest(new BytesArray(writeRequest.toByteArray())));

        assertThat(response.getStatus(), equalTo(RestStatus.NO_CONTENT));
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

        RemoteWriteResponse response = executeRequest(new RemoteWriteRequest(new BytesArray(writeRequest.toByteArray())));

        assertThat(response.getStatus(), equalTo(RestStatus.NO_CONTENT));
    }

    public void test429() {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("metrics-generic.prometheus-default", RestStatus.TOO_MANY_REQUESTS, "too many requests"),
            successResponse() };

        RemoteWriteResponse response = executeRequest(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new BulkResponse(bulkItemResponses, 0)
        );

        assertThat(response.getStatus(), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testPartialSuccess() {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("metrics-generic.prometheus-default", RestStatus.BAD_REQUEST, "bad request"),
            successResponse() };

        RemoteWriteResponse response = executeRequest(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new BulkResponse(bulkItemResponses, 0)
        );

        // Per remote write spec: MUST return 400 for requests containing invalid samples
        assertThat(response.getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertNotNull(response.getMessage());
        assertThat(response.getMessage(), containsString("bad request"));
    }

    public void testBulkFailure() {
        RemoteWriteResponse response = executeRequest(
            createWriteRequest("test_metric", 42.0, System.currentTimeMillis()),
            new IllegalStateException("bulk failure")
        );

        assertThat(response.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testInvalidProtobufReturns400() {
        // Invalid protobuf bytes should return 400 Bad Request (not 500)
        // per Prometheus remote write spec: 4xx for invalid requests that should not be retried
        RemoteWriteRequest request = new RemoteWriteRequest(new BytesArray(new byte[] { 0x00, 0x01, 0x02, 0x03 }));

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<RemoteWriteResponse> response = ArgumentCaptor.forClass(RemoteWriteResponse.class);
        verify(responseListener).onResponse(response.capture());
        assertThat(response.getValue().getStatus(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testTimeseriesWithoutNameLabelReturns400() {
        // Timeseries without __name__ label are invalid samples
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(RemoteWrite.Label.newBuilder().setName("job").setValue("test").build())
                    .addSamples(RemoteWrite.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                    .build()
            )
            .build();

        RemoteWriteResponse response = executeRequest(new RemoteWriteRequest(new BytesArray(writeRequest.toByteArray())));

        // Per remote write spec: MUST return 400 for requests containing invalid samples
        assertThat(response.getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertNotNull(response.getMessage());
        assertThat(response.getMessage(), containsString("missing __name__ label"));
    }

    public void testPartialSuccessWithDroppedSamples() {
        // Mix of valid timeseries and timeseries without __name__ label, bulk succeeds for valid ones
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

        RemoteWriteResponse response = executeRequest(new RemoteWriteRequest(new BytesArray(writeRequest.toByteArray())));

        // Per remote write spec: MUST return 400 for requests containing invalid samples
        assertThat(response.getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertNotNull(response.getMessage());
        assertThat(response.getMessage(), containsString("missing __name__ label"));
    }

    public void testInferMetricType() {
        // Counter metrics
        assertThat(PrometheusRemoteWriteTransportAction.inferMetricType("http_requests_total"), equalTo("counter"));
        assertThat(PrometheusRemoteWriteTransportAction.inferMetricType("request_duration_bucket"), equalTo("counter"));

        // Gauge metrics
        assertThat(PrometheusRemoteWriteTransportAction.inferMetricType("temperature"), equalTo("gauge"));
        assertThat(PrometheusRemoteWriteTransportAction.inferMetricType("memory_usage"), equalTo("gauge"));
        assertThat(PrometheusRemoteWriteTransportAction.inferMetricType("cpu_total_percent"), equalTo("gauge")); // "total" in middle
    }

    private RemoteWriteResponse executeRequest(RemoteWriteRequest request) {
        return executeRequest(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 0)));
    }

    private RemoteWriteResponse executeRequest(RemoteWriteRequest request, BulkResponse bulkResponse) {
        return executeRequest(request, listener -> listener.onResponse(bulkResponse));
    }

    private RemoteWriteResponse executeRequest(RemoteWriteRequest request, Exception bulkFailure) {
        return executeRequest(request, listener -> listener.onFailure(bulkFailure));
    }

    private RemoteWriteResponse executeRequest(RemoteWriteRequest request, Consumer<ActionListener<BulkResponse>> bulkResponseConsumer) {
        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), any(), bulkResponseListener.capture());

        @SuppressWarnings("unchecked")
        ActionListener<RemoteWriteResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        if (bulkResponseListener.getAllValues().isEmpty() == false) {
            bulkResponseConsumer.accept(bulkResponseListener.getValue());
        }

        ArgumentCaptor<RemoteWriteResponse> response = ArgumentCaptor.forClass(RemoteWriteResponse.class);
        verify(responseListener).onResponse(response.capture());
        return response.getValue();
    }

    private static RemoteWriteRequest createEmptyWriteRequest() {
        return new RemoteWriteRequest(new BytesArray(RemoteWrite.WriteRequest.newBuilder().build().toByteArray()));
    }

    private static RemoteWriteRequest createWriteRequest(String metricName, double value, long timestamp) {
        return new RemoteWriteRequest(
            new BytesArray(
                RemoteWrite.WriteRequest.newBuilder().addTimeseries(createTimeSeries(metricName, value, timestamp)).build().toByteArray()
            )
        );
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
