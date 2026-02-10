/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Label;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Sample;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.TimeSeries;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport action for handling Prometheus Remote Write requests.
 * This action processes incoming metrics data in Prometheus remote write format.
 *
 * @see <a href="https://prometheus.io/docs/concepts/remote_write_spec/">Prometheus Remote Write Specification</a>
 */
public class PrometheusRemoteWriteTransportAction extends HandledTransportAction<
    PrometheusRemoteWriteTransportAction.RemoteWriteRequest,
    PrometheusRemoteWriteTransportAction.RemoteWriteResponse> {

    public static final String NAME = "indices:data/write/prometheus/remote_write";
    public static final ActionType<RemoteWriteResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(PrometheusRemoteWriteTransportAction.class);

    private static final String METRIC_NAME_LABEL = "__name__";
    private static final String DYNAMIC_TEMPLATE_COUNTER = "counter";
    private static final String DYNAMIC_TEMPLATE_GAUGE = "gauge";
    private static final String METRICS_DATA_STREAM_PREFIX = "metrics-";

    private final Client client;

    @Inject
    public PrometheusRemoteWriteTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, RemoteWriteRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, RemoteWriteRequest request, ActionListener<RemoteWriteResponse> listener) {
        try {
            RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.parseFrom(request.remoteWriteRequest.streamInput());

            int totalSamples = countTotalSamples(writeRequest);
            if (totalSamples == 0) {
                listener.onResponse(new RemoteWriteResponse(RestStatus.NO_CONTENT));
                return;
            }

            logger.debug(
                "Received Prometheus remote write request with {} timeseries, {} samples",
                writeRequest.getTimeseriesCount(),
                totalSamples
            );

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            int droppedMissingName = 0;
            for (TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                String metricName = extractMetricName(timeSeries.getLabelsList());
                if (metricName == null) {
                    droppedMissingName += timeSeries.getSamplesCount();
                    continue;
                }

                String dynamicTemplate = inferMetricType(metricName);

                for (Sample sample : timeSeries.getSamplesList()) {
                    IndexRequest indexRequest = buildIndexRequest(
                        timeSeries,
                        sample,
                        metricName,
                        dynamicTemplate,
                        request.dataset,
                        request.namespace
                    );
                    bulkRequestBuilder.add(indexRequest);
                }
            }

            if (bulkRequestBuilder.numberOfActions() == 0) {
                String message = buildFailureSummary(totalSamples, droppedMissingName, droppedMissingName, Map.of());
                listener.onResponse(new RemoteWriteResponse(RestStatus.BAD_REQUEST, message));
                return;
            }

            final int finalDroppedMissingName = droppedMissingName;
            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures() || finalDroppedMissingName > 0) {
                        handlePartialSuccess(bulkResponse, totalSamples, finalDroppedMissingName, listener);
                    } else {
                        listener.onResponse(new RemoteWriteResponse(RestStatus.NO_CONTENT));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onResponse(new RemoteWriteResponse(ExceptionsHelper.status(e)));
                }
            });

        } catch (InvalidProtocolBufferException e) {
            // Invalid protobuf is a client error - return 400 so clients don't retry
            listener.onResponse(new RemoteWriteResponse(RestStatus.BAD_REQUEST));
        } catch (Exception e) {
            logger.error("Failed to process Prometheus remote write request", e);
            listener.onResponse(new RemoteWriteResponse(ExceptionsHelper.status(e)));
        }
    }

    private static int countTotalSamples(RemoteWrite.WriteRequest writeRequest) {
        int count = 0;
        for (TimeSeries ts : writeRequest.getTimeseriesList()) {
            count += ts.getSamplesCount();
        }
        return count;
    }

    private static String extractMetricName(List<Label> labels) {
        for (Label label : labels) {
            if (METRIC_NAME_LABEL.equals(label.getName())) {
                return label.getValue();
            }
        }
        return null;
    }

    /**
     * Infers the metric type based on naming conventions.
     * Metrics ending with "_total", "_bucket", "_count", or "_sum" are treated as counters,
     * all other metrics are gauges.
     */
    static String inferMetricType(String metricName) {
        if (metricName.endsWith("_total")
            || metricName.endsWith("_bucket")
            || metricName.endsWith("_count")
            || metricName.endsWith("_sum")) {
            return DYNAMIC_TEMPLATE_COUNTER;
        }
        return DYNAMIC_TEMPLATE_GAUGE;
    }

    private IndexRequest buildIndexRequest(
        TimeSeries timeSeries,
        Sample sample,
        String metricName,
        String dynamicTemplate,
        String dataset,
        String namespace
    ) throws IOException {
        try (XContentBuilder builder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            builder.startObject();

            // @timestamp - Prometheus timestamps are in milliseconds
            builder.field("@timestamp", sample.getTimestamp());

            // data_stream fields
            String fullDataset = dataset + ".prometheus";
            builder.startObject("data_stream");
            builder.field("type", "metrics");
            builder.field("dataset", fullDataset);
            builder.field("namespace", namespace);
            builder.endObject();

            // labels - all labels including __name__
            builder.startObject("labels");
            for (Label label : timeSeries.getLabelsList()) {
                builder.field(label.getName(), label.getValue());
            }
            builder.endObject();

            // metric value - field named after the metric
            builder.field(metricName, sample.getValue());

            builder.endObject();

            String targetIndex = METRICS_DATA_STREAM_PREFIX + fullDataset + "-" + namespace;
            return new IndexRequest(targetIndex).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .source(builder)
                .setDynamicTemplates(Map.of(metricName, dynamicTemplate));
        }
    }

    private static void handlePartialSuccess(
        BulkResponse bulkResponse,
        int totalSamples,
        int droppedMissingName,
        ActionListener<RemoteWriteResponse> listener
    ) {
        // Count failures and group by status
        Map<String, Map<RestStatus, FailureGroup>> failureGroups = null;
        // Default to returning 400 per the remote write spec for requests that should not be retried.
        RestStatus responseStatus = RestStatus.BAD_REQUEST;
        int failures = droppedMissingName;

        for (BulkItemResponse item : bulkResponse.getItems()) {
            BulkItemResponse.Failure failure = item.getFailure();
            if (failure != null) {
                failures++;
                if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                    // 429 takes priority so clients retry (valid samples that were rate-limited may succeed on retry).
                    responseStatus = RestStatus.TOO_MANY_REQUESTS;
                }
                if (failureGroups == null) {
                    failureGroups = new HashMap<>();
                }
                failureGroups.computeIfAbsent(failure.getIndex(), k -> new HashMap<>())
                    .computeIfAbsent(failure.getStatus(), k -> new FailureGroup(new AtomicInteger(0), failure.getMessage()))
                    .failureCount()
                    .incrementAndGet();
            }
        }

        String message = buildFailureSummary(totalSamples, droppedMissingName, failures, failureGroups);
        listener.onResponse(new RemoteWriteResponse(responseStatus, message));
    }

    private static String buildFailureSummary(
        int totalSamples,
        int droppedMissingName,
        int failures,
        @Nullable Map<String, Map<RestStatus, FailureGroup>> failureGroups
    ) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Prometheus remote write request partially failed: ")
            .append(failures)
            .append(" of ")
            .append(totalSamples)
            .append(" samples failed.\n");
        if (droppedMissingName > 0) {
            failureMessage.append(droppedMissingName).append(" sample(s) dropped due to missing __name__ label\n");
        }
        if (failureGroups != null) {
            for (Map.Entry<String, Map<RestStatus, FailureGroup>> indexEntry : failureGroups.entrySet()) {
                for (Map.Entry<RestStatus, FailureGroup> statusEntry : indexEntry.getValue().entrySet()) {
                    FailureGroup group = statusEntry.getValue();
                    failureMessage.append("Index [")
                        .append(indexEntry.getKey())
                        .append("] returned status [")
                        .append(statusEntry.getKey())
                        .append("] for ")
                        .append(group.failureCount())
                        .append(" documents. Sample error: ")
                        .append(group.failureMessageSample())
                        .append("\n");
                }
            }
        }

        return failureMessage.toString();
    }

    record FailureGroup(AtomicInteger failureCount, String failureMessageSample) {}

    public static class RemoteWriteRequest extends ActionRequest implements CompositeIndicesRequest {
        private final BytesReference remoteWriteRequest;
        private final String dataset;
        private final String namespace;

        public RemoteWriteRequest(StreamInput in) throws IOException {
            super(in);
            remoteWriteRequest = in.readBytesReference();
            dataset = in.readString();
            namespace = in.readString();
        }

        public RemoteWriteRequest(BytesReference remoteWriteRequest, String dataset, String namespace) {
            this.remoteWriteRequest = remoteWriteRequest;
            this.dataset = dataset;
            this.namespace = namespace;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(remoteWriteRequest);
            out.writeString(dataset);
            out.writeString(namespace);
        }
    }

    public static class RemoteWriteResponse extends ActionResponse {
        private final RestStatus status;
        @Nullable
        private final String message;

        public RemoteWriteResponse(RestStatus status) {
            this(status, null);
        }

        public RemoteWriteResponse(RestStatus status, @Nullable String message) {
            this.status = status;
            this.message = message;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(status);
        }

        public RestStatus getStatus() {
            return status;
        }

        @Nullable
        public String getMessage() {
            return message;
        }
    }
}
