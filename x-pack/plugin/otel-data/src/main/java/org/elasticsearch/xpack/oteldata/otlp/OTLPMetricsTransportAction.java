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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(OTLPMetricsTransportAction.class);
    public static final int IGNORED_DATA_POINTS_MESSAGE_LIMIT = 10;
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
            if (context.totalDataPoints() == 0) {
                handleEmptyRequest(listener);
                return;
            }
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor);
            context.consume(dataPointGroup -> addIndexRequest(bulkRequestBuilder, metricDocumentBuilder, dataPointGroup));
            if (bulkRequestBuilder.numberOfActions() == 0) {
                // all data points were ignored
                handlePartialSuccess(listener, context);
                return;
            }

            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    if (bulkItemResponses.hasFailures() || context.getIgnoredDataPoints() > 0) {
                        handlePartialSuccess(bulkItemResponses, context, listener);
                    } else {
                        handleSuccess(listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(listener, e, context);
                }
            });

        } catch (Exception e) {
            logger.error("failed to execute otlp metrics request", e);
            handleFailure(listener, e, context);
        }
    }

    private void addIndexRequest(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            Map<String, String> dynamicTemplates = Maps.newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            BytesRef tsid = metricDocumentBuilder.buildMetricDocument(xContentBuilder, dynamicTemplates, dataPointGroup);
            bulkRequestBuilder.add(
                new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                    .setRequireDataStream(true)
                    .source(xContentBuilder)
                    .tsid(tsid)
                    .setIncludeSourceOnError(false)
                    .setDynamicTemplates(dynamicTemplates)
            );
        }
    }

    private static void handleSuccess(ActionListener<MetricsResponse> listener) {
        listener.onResponse(new MetricsResponse(RestStatus.OK, ExportMetricsServiceResponse.newBuilder().build()));
    }

    private static void handleEmptyRequest(ActionListener<MetricsResponse> listener) {
        // If the server receives an empty request
        // (a request that does not carry any telemetry data)
        // the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        handleSuccess(listener);
    }

    private static void handlePartialSuccess(ActionListener<MetricsResponse> listener, DataPointGroupingContext context) {
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        ExportMetricsServiceResponse response = responseWithRejectedDataPoints(
            context.getIgnoredDataPoints(),
            context.getIgnoredDataPointsMessage(IGNORED_DATA_POINTS_MESSAGE_LIMIT)
        );
        listener.onResponse(new MetricsResponse(RestStatus.BAD_REQUEST, response));
    }

    private static void handlePartialSuccess(
        BulkResponse bulkItemResponses,
        DataPointGroupingContext context,
        ActionListener<MetricsResponse> listener
    ) {
        // index -> status -> failure group
        Map<String, Map<RestStatus, FailureGroup>> failureGroups = new HashMap<>();
        // If the request is only partially accepted
        // (i.e. when the server accepts only parts of the data and rejects the rest),
        // the server MUST respond with HTTP 200 OK.
        // https://opentelemetry.io/docs/specs/otlp/#partial-success-1
        RestStatus status = RestStatus.OK;
        int failures = 0;
        for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
            if (failure != null) {
                // we're counting each document as one data point here
                // which is an approximation since one document can represent multiple data points
                failures++;
                if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                    // If the server receives more requests than the client is allowed or the server is overloaded,
                    // the server SHOULD respond with HTTP 429 Too Many Requests or HTTP 503 Service Unavailable
                    // and MAY include “Retry-After” header with a recommended time interval in seconds to wait before retrying.
                    // https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling
                    status = RestStatus.TOO_MANY_REQUESTS;
                }
                FailureGroup failureGroup = failureGroups.computeIfAbsent(failure.getIndex(), k -> new HashMap<>())
                    .computeIfAbsent(failure.getStatus(), k -> new FailureGroup(new AtomicInteger(0), failure.getMessage()));
                failureGroup.failureCount().incrementAndGet();
            }
        }
        if (bulkItemResponses.getItems().length == failures) {
            // all data points failed, so we report total data points as failures
            failures = context.totalDataPoints();
        }
        StringBuilder failureMessageBuilder = new StringBuilder();
        for (Map.Entry<String, Map<RestStatus, FailureGroup>> indexEntry : failureGroups.entrySet()) {
            String indexName = indexEntry.getKey();
            for (Map.Entry<RestStatus, FailureGroup> statusEntry : indexEntry.getValue().entrySet()) {
                RestStatus restStatus = statusEntry.getKey();
                FailureGroup failureGroup = statusEntry.getValue();
                failureMessageBuilder.append("Index [")
                    .append(indexName)
                    .append("] returned status [")
                    .append(restStatus)
                    .append("] for ")
                    .append(failureGroup.failureCount())
                    .append(" documents. Sample error message: ");
                failureMessageBuilder.append(failureGroup.failureMessageSample());
                failureMessageBuilder.append("\n");
            }
        }
        failureMessageBuilder.append(context.getIgnoredDataPointsMessage(10));
        ExportMetricsServiceResponse response = responseWithRejectedDataPoints(
            failures + context.getIgnoredDataPoints(),
            failureMessageBuilder.toString()
        );
        listener.onResponse(new MetricsResponse(status, response));
    }

    record FailureGroup(AtomicInteger failureCount, String failureMessageSample) {}

    private static void handleFailure(ActionListener<MetricsResponse> listener, Exception e, DataPointGroupingContext context) {
        // https://opentelemetry.io/docs/specs/otlp/#failures-1
        // If the processing of the request fails,
        // the server MUST respond with appropriate HTTP 4xx or HTTP 5xx status code.
        listener.onResponse(
            new MetricsResponse(ExceptionsHelper.status(e), responseWithRejectedDataPoints(context.totalDataPoints(), e.getMessage()))
        );
    }

    private static ExportMetricsServiceResponse responseWithRejectedDataPoints(int rejectedDataPoints, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedDataPoints)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }

    public static class MetricsRequest extends ActionRequest implements CompositeIndicesRequest {
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
        private final BytesReference response;
        private final RestStatus status;

        public MetricsResponse(RestStatus status, ExportMetricsServiceResponse response) {
            this(status, new BytesArray(response.toByteArray()));
        }

        public MetricsResponse(RestStatus status, BytesReference response) {
            this.response = response;
            this.status = status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(response);
            out.writeEnum(status);
        }

        public BytesReference getResponse() {
            return response;
        }

        public RestStatus getStatus() {
            return status;
        }
    }
}
