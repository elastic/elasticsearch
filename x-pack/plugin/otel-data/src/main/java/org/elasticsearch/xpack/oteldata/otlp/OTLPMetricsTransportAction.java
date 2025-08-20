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

import com.google.protobuf.MessageLite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointGroupTsidFunnel;

import java.io.IOException;
import java.util.Arrays;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends HandledTransportAction<
    OTLPMetricsTransportAction.MetricsRequest,
    OTLPMetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(OTLPMetricsTransportAction.class);
    private final Client client;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor);
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());
            context.groupDataPoints(metricsServiceRequest);
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor);
            context.consume(dataPointGroup -> addIndexRequest(bulkRequestBuilder, metricDocumentBuilder, dataPointGroup));
            if (bulkRequestBuilder.numberOfActions() == 0) {
                if (context.totalDataPoints() == 0) {
                    // If the server receives an empty request
                    // (a request that does not carry any telemetry data)
                    // the server SHOULD respond with success.
                    // https://opentelemetry.io/docs/specs/otlp/#full-success-1
                    listener.onResponse(new MetricsResponse(RestStatus.OK, ExportMetricsServiceResponse.newBuilder().build()));
                } else {
                    // If the processing of the request fails because the request contains data that cannot be decoded
                    // or is otherwise invalid and such failure is permanent,
                    // then the server MUST respond with HTTP 400 Bad Request.
                    // https://opentelemetry.io/docs/specs/otlp/#bad-data
                    listener.onResponse(
                        new MetricsResponse(
                            RestStatus.BAD_REQUEST,
                            responseWithRejectedDataPoints(context.totalDataPoints(), context.getIgnoredDataPointsMessage())
                        )
                    );
                }
                return;
            }

            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    MessageLite response;
                    // If the request is only partially accepted
                    // (i.e. when the server accepts only parts of the data and rejects the rest),
                    // the server MUST respond with HTTP 200 OK.
                    // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
                    RestStatus status = RestStatus.OK;
                    if (bulkItemResponses.hasFailures() || context.getIgnoredDataPoints() > 0) {
                        int failures = (int) Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed).count();
                        if (failures == bulkItemResponses.getItems().length) {
                            // If the processing of the request fails,
                            // the server MUST respond with appropriate HTTP 4xx or HTTP 5xx status code.
                            // https://opentelemetry.io/docs/specs/otlp/#failures-1
                            status = RestStatus.INTERNAL_SERVER_ERROR;
                        }
                        response = responseWithRejectedDataPoints(
                            failures + context.getIgnoredDataPoints(),
                            bulkItemResponses.buildFailureMessage() + context.getIgnoredDataPointsMessage()
                        );
                    } else {
                        response = ExportMetricsServiceResponse.newBuilder().build();
                    }
                    listener.onResponse(new MetricsResponse(status, response));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onResponse(
                        // https://opentelemetry.io/docs/specs/otlp/#failures-1
                        // If the processing of the request fails,
                        // the server MUST respond with appropriate HTTP 4xx or HTTP 5xx status code.
                        new MetricsResponse(
                            RestStatus.INTERNAL_SERVER_ERROR,
                            responseWithRejectedDataPoints(context.totalDataPoints(), e.getMessage())
                        )
                    );
                }
            });

        } catch (Exception e) {
            logger.error("failed to execute otlp metrics request", e);
            new MetricsResponse(
                RestStatus.INTERNAL_SERVER_ERROR,
                responseWithRejectedDataPoints(context.totalDataPoints(), e.getMessage())
            );
        }
    }

    private static ExportMetricsPartialSuccess responseWithRejectedDataPoints(int rejectedDataPoints, String message) {
        return ExportMetricsServiceResponse.newBuilder()
            .getPartialSuccessBuilder()
            .setRejectedDataPoints(rejectedDataPoints)
            .setErrorMessage(message)
            .build();
    }

    private void addIndexRequest(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = metricDocumentBuilder.buildMetricDocument(xContentBuilder, dataPointGroup);
            bulkRequestBuilder.add(
                new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                    .setRequireDataStream(true)
                    .source(xContentBuilder)
                    .tsid(DataPointGroupTsidFunnel.forDataPointGroup(dataPointGroup).buildTsid())
                    .setDynamicTemplates(dynamicTemplates)
            );
        }
    }

    public static class MetricsRequest extends ActionRequest {
        private final BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(BytesReference exportMetricsServiceRequest) {
            this.exportMetricsServiceRequest = exportMetricsServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class MetricsResponse extends ActionResponse {
        private final MessageLite response;
        private final RestStatus status;

        public MetricsResponse(RestStatus status, MessageLite response) {
            this.response = response;
            this.status = status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.write(response.toByteArray());
        }

        public MessageLite getResponse() {
            return response;
        }

        public RestStatus getStatus() {
            return status;
        }
    }
}
