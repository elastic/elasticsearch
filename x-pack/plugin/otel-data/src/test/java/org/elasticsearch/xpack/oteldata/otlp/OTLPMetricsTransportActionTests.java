/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.InvalidProtocolBufferException;

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
import org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction.MetricsResponse;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTLPMetricsTransportActionTests extends ESTestCase {

    private OTLPMetricsTransportAction action;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));

        action = new OTLPMetricsTransportAction(mock(TransportService.class), mock(ActionFilters.class), mock(ThreadPool.class), client);
    }

    public void testSuccess() throws Exception {
        MetricsResponse response = executeRequest(createMetricsRequest(createMetric()));

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportMetricsServiceResponse metricsServiceResponse = ExportMetricsServiceResponse.parseFrom(response.getResponse().array());
        assertThat(metricsServiceResponse.hasPartialSuccess(), equalTo(false));
    }

    public void testSuccessEmptyRequest() throws Exception {
        MetricsResponse response = executeRequest(createMetricsRequest());

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportMetricsServiceResponse metricsServiceResponse = ExportMetricsServiceResponse.parseFrom(response.getResponse().array());
        assertThat(metricsServiceResponse.hasPartialSuccess(), equalTo(false));
    }

    public void test429() throws Exception {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("metrics-generic.otel-default", RestStatus.TOO_MANY_REQUESTS, "too many requests"),
            successResponse() };
        MetricsResponse response = executeRequest(createMetricsRequest(createMetric()), new BulkResponse(bulkItemResponses, 0));

        assertThat(response.getStatus(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        ExportMetricsPartialSuccess metricsServiceResponse = ExportMetricsServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(
            metricsServiceResponse.getRejectedDataPoints(),
            equalTo(Arrays.stream(bulkItemResponses).filter(BulkItemResponse::isFailed).count())
        );
        assertThat(metricsServiceResponse.getErrorMessage(), containsString("too many requests"));
    }

    public void testPartialSuccess() throws Exception {
        MetricsResponse response = executeRequest(
            createMetricsRequest(createMetric()),
            new BulkResponse(
                new BulkItemResponse[] {
                    failureResponse("metrics-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 1"),
                    failureResponse("metrics-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 2"),
                    failureResponse("metrics-hostmetricsreceiver.otel-default", RestStatus.BAD_REQUEST, "bad request 3"),
                    failureResponse("metrics-generic.otel-default", RestStatus.INTERNAL_SERVER_ERROR, "internal server error") },
                0
            )
        );

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportMetricsPartialSuccess metricsServiceResponse = ExportMetricsServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(metricsServiceResponse.getRejectedDataPoints(), equalTo(1L));
        // the error message contains only one message per unique index and error status
        assertThat(metricsServiceResponse.getErrorMessage(), containsString("bad request 1"));
        assertThat(metricsServiceResponse.getErrorMessage(), not(containsString("bad request  2")));
        assertThat(metricsServiceResponse.getErrorMessage(), containsString("bad request 3"));
        assertThat(metricsServiceResponse.getErrorMessage(), containsString("internal server error"));
    }

    public void testBulkError() throws Exception {
        assertExceptionStatus(new IllegalArgumentException("bazinga"), RestStatus.BAD_REQUEST);
        assertExceptionStatus(new IllegalStateException("bazinga"), RestStatus.INTERNAL_SERVER_ERROR);
    }

    private void assertExceptionStatus(Exception exception, RestStatus restStatus) throws InvalidProtocolBufferException {
        if (randomBoolean()) {
            doThrow(exception).when(client).execute(any(), any(), any());
        }
        MetricsResponse response = executeRequest(createMetricsRequest(createMetric()), exception);

        assertThat(response.getStatus(), equalTo(restStatus));
        ExportMetricsPartialSuccess metricsServiceResponse = ExportMetricsServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(metricsServiceResponse.getRejectedDataPoints(), equalTo(1L));
        assertThat(metricsServiceResponse.getErrorMessage(), equalTo(exception.getMessage()));
    }

    private MetricsResponse executeRequest(OTLPMetricsTransportAction.MetricsRequest request) {
        return executeRequest(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 0)));
    }

    private MetricsResponse executeRequest(OTLPMetricsTransportAction.MetricsRequest request, BulkResponse bulkResponse) {
        return executeRequest(request, listener -> listener.onResponse(bulkResponse));
    }

    private MetricsResponse executeRequest(OTLPMetricsTransportAction.MetricsRequest request, Exception bulkFailure) {
        return executeRequest(request, listener -> listener.onFailure(bulkFailure));
    }

    private MetricsResponse executeRequest(
        OTLPMetricsTransportAction.MetricsRequest request,
        Consumer<ActionListener<BulkResponse>> bulkResponseConsumer
    ) {
        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), any(), bulkResponseListener.capture());

        ActionListener<MetricsResponse> metricsResponseListener = mock();
        action.doExecute(null, request, metricsResponseListener);
        if (bulkResponseListener.getAllValues().isEmpty() == false) {
            bulkResponseConsumer.accept(bulkResponseListener.getValue());
        }

        ArgumentCaptor<MetricsResponse> response = ArgumentCaptor.forClass(MetricsResponse.class);
        verify(metricsResponseListener).onResponse(response.capture());
        return response.getValue();
    }

    private static OTLPMetricsTransportAction.MetricsRequest createMetricsRequest(Metric... metrics) {
        return new OTLPMetricsTransportAction.MetricsRequest(
            new BytesArray(
                ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(
                        OtlpUtils.createResourceMetrics(
                            List.of(keyValue("service.name", "test-service")),
                            List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metrics)))
                        )
                    )
                    .build()
                    .toByteArray()
            )
        );
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
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
