/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.protobuf.MessageLite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetricsDBTransportAction extends HandledTransportAction<
    MetricsDBTransportAction.MetricsRequest,
    MetricsDBTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsDBTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsDBTransportAction.class);
    private static final Hasher128 HASHER = Hashing.murmur3_128();
    private final Client client;

    @Inject
    public MetricsDBTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool, Client client) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            addIndexRequests(bulkRequestBuilder, metricsServiceRequest);
            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    MessageLite response;
                    if (bulkItemResponses.hasFailures()) {
                        long failures = Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed).count();
                        response = ExportMetricsServiceResponse.newBuilder()
                            .getPartialSuccessBuilder()
                            .setRejectedDataPoints(failures)
                            .setErrorMessage(bulkItemResponses.buildFailureMessage())
                            .build();
                    } else {
                        response = ExportMetricsServiceResponse.newBuilder().build();
                    }
                    listener.onResponse(new MetricsResponse(response));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void addIndexRequests(BulkRequestBuilder bulkRequestBuilder, ExportMetricsServiceRequest exportMetricsServiceRequest)
        throws IOException {
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            List<KeyValue> resourceAttributes = resourceMetrics.getResource().getAttributesList();
            HashValue128 resourceAttributesHash = HASHER.hashTo128Bits(resourceAttributes, AttributeListHashFunnel.get());
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                List<KeyValue> scopeAttributes = scopeMetrics.getScope().getAttributesList();
                HashValue128 scopeAttributesHash = HASHER.hashTo128Bits(scopeAttributes, AttributeListHashFunnel.get());
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
                    switch (metric.getDataCase()) {
                        case SUM -> createNumberDataPointDocs(
                            resourceAttributes,
                            resourceAttributesHash,
                            scopeAttributes,
                            scopeAttributesHash,
                            metric,
                            metric.getSum().getDataPointsList(),
                            metric.getSum().getAggregationTemporality(),
                            bulkRequestBuilder
                        );
                        case GAUGE -> createNumberDataPointDocs(
                            resourceAttributes,
                            resourceAttributesHash,
                            scopeAttributes,
                            scopeAttributesHash,
                            metric,
                            metric.getGauge().getDataPointsList(),
                            null,
                            bulkRequestBuilder
                        );
                        default -> throw new IllegalArgumentException("Unsupported metric type [" + metric.getDataCase() + "]");
                    }
                }
            }
        }
    }

    private void createNumberDataPointDocs(
        List<KeyValue> resourceAttributes,
        HashValue128 resourceAttributesHash,
        List<KeyValue> scopeAttributes,
        HashValue128 scopeAttributesHash,
        Metric metric,
        List<NumberDataPoint> dataPoints,
        @Nullable AggregationTemporality temporality,
        BulkRequestBuilder bulkRequestBuilder
    ) throws IOException {
        for (int i = 0; i < dataPoints.size(); i++) {
            NumberDataPoint dp = dataPoints.get(i);
            try (XContentBuilder xContentBuilder = CborXContent.contentBuilder()) {
                buildDataPointDoc(
                    xContentBuilder,
                    resourceAttributes,
                    resourceAttributesHash,
                    scopeAttributes,
                    scopeAttributesHash,
                    metric,
                    dp
                );
                bulkRequestBuilder.add(
                    client.prepareIndex("metricsdb")
                        .setCreate(true)
                        .setRequireDataStream(true)
                        .setPipeline(IngestService.NOOP_PIPELINE_NAME)
                        .setSource(xContentBuilder)
                );
            }
        }
    }

    private void buildDataPointDoc(
        XContentBuilder builder,
        List<KeyValue> resourceAttributes,
        HashValue128 resourceAttributesHash,
        List<KeyValue> scopeAttributes,
        HashValue128 scopeAttributesHash,
        Metric metric,
        NumberDataPoint dp
    ) throws IOException {
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(dp.getTimeUnixNano()));
        builder.startObject("resource");
        builder.startObject("attributes");
        buildAttributes(builder, resourceAttributes);
        builder.endObject();
        builder.endObject();
        builder.startObject("scope");
        builder.startObject("attributes");
        buildAttributes(builder, scopeAttributes);
        builder.endObject();
        builder.endObject();
        builder.startObject("attributes");
        buildAttributes(builder, resourceAttributes);
        builder.endObject();
        builder.field("unit", metric.getUnit());
        builder.field("type");
        if (metric.getDataCase() == Metric.DataCase.SUM) {
            if (metric.getSum().getIsMonotonic() == false) {
                builder.value("up_down_counter");
            } else {
                builder.value("counter");
            }
        } else {
            builder.value(Strings.toLowercaseAscii(metric.getDataCase().toString()));
        }
        switch (metric.getDataCase()) {
            case SUM -> builder.field("temporality", metric.getSum().getAggregationTemporality().toString());
            case HISTOGRAM -> builder.field("temporality", metric.getHistogram().getAggregationTemporality().toString());
            case EXPONENTIAL_HISTOGRAM -> builder.field(
                "temporality",
                metric.getExponentialHistogram().getAggregationTemporality().toString()
            );
        }
        builder.startObject("metric");
        builder.field("name", metric.getName());
        builder.startObject("value");
        switch (dp.getValueCase()) {
            case AS_DOUBLE -> builder.field("double", dp.getAsDouble());
            case AS_INT -> builder.field("long", dp.getAsInt());
        }
        builder.endObject();
        builder.endObject();
        builder.endObject();
    }

    private void buildAttributes(XContentBuilder builder, List<KeyValue> resourceAttributes) throws IOException {
        for (KeyValue attribute : resourceAttributes) {

            builder.field(attribute.getKey());
            attributeValue(builder, attribute.getValue());
        }
    }

    private void attributeValue(XContentBuilder builder, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> builder.value(value.getStringValue());
            case BOOL_VALUE -> builder.value(value.getBoolValue());
            case INT_VALUE -> builder.value(value.getIntValue());
            case DOUBLE_VALUE -> builder.value(value.getDoubleValue());
            case ARRAY_VALUE -> {
                builder.startArray();
                for (AnyValue arrayValue : value.getArrayValue().getValuesList()) {
                    attributeValue(builder, arrayValue);
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

    public static class MetricsRequest extends ActionRequest {
        private final boolean normalized;
        private final boolean noop;
        private final BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            normalized = in.readBoolean();
            noop = in.readBoolean();
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(boolean normalized, boolean noop, BytesReference exportMetricsServiceRequest) {
            this.normalized = normalized;
            this.noop = noop;
            this.exportMetricsServiceRequest = exportMetricsServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class MetricsResponse extends ActionResponse {
        private final MessageLite response;

        public MetricsResponse(MessageLite response) {
            this.response = response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.write(response.toByteArray());
        }

        public MessageLite getResponse() {
            return response;
        }
    }
}
