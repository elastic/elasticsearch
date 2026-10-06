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
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.prometheus.PrometheusPlugin;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Exemplar;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Label;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Sample;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport action for handling Prometheus Remote Write requests.
 * This action processes incoming metrics data in Prometheus remote write format.
 * <p>
 * If a time series repeats routing labels, behavior is undefined; the implementation applies updates in label
 * order so the last non-empty value observed wins in practice.
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
    private static final String METRICS_DATA_STREAM_PREFIX = "metrics-";
    private static final String EXEMPLARS_DATA_STREAM_PREFIX = "exemplars-";
    private static final String DATA_STREAM_DATASET_LABEL = "data_stream_dataset";
    private static final String DATA_STREAM_NAMESPACE_LABEL = "data_stream_namespace";
    private static final String PROMETHEUS_DATASET_SUFFIX = ".prometheus";

    private final Client client;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final long maxExpandedContentLength;

    @Inject
    public PrometheusRemoteWriteTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        BigArrays bigArrays,
        Settings settings
    ) {
        super(NAME, transportService, actionFilters, in -> TransportAction.localOnly(), threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.maxExpandedContentLength = HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.get(settings).getBytes();
    }

    @Override
    protected void doExecute(Task task, RemoteWriteRequest request, ActionListener<RemoteWriteResponse> listener) {
        try (request; LongHash exemplarTimestamps = new LongHash(1, bigArrays)) {
            RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.parseFrom(request.remoteWriteRequest.streamInput());
            request.releaseBody();

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            boolean exemplarIngestionEnabled = PrometheusPlugin.METRIC_EXEMPLARS_FEATURE_FLAG.isEnabled();
            long requestTimestamp = exemplarIngestionEnabled ? threadPool.absoluteTimeInMillis() : 0;

            int totalSamples = 0;
            int totalExemplars = 0;
            int duplicateExemplars = 0;
            int droppedSamplesMissingName = 0;
            int droppedExemplarsMissingName = 0;
            ExpandedContentTracker expandedContentTracker = new ExpandedContentTracker(maxExpandedContentLength);
            List<IndexRequest> sampleRequests = new ArrayList<>();
            List<IndexRequest> exemplarRequests = new ArrayList<>();
            for (TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                exemplarTimestamps.clear();
                int seriesSamples = timeSeries.getSamplesCount();
                int seriesExemplars = exemplarIngestionEnabled ? timeSeries.getExemplarsCount() : 0;
                totalSamples += seriesSamples;
                totalExemplars += seriesExemplars;

                String metricName = null;
                String dataset = request.dataset;
                String namespace = request.namespace;
                for (Label label : timeSeries.getLabelsList()) {
                    String labelValue = label.getValue();
                    if (Strings.hasText(labelValue) == false) {
                        continue;
                    }
                    String labelName = label.getName();
                    if (METRIC_NAME_LABEL.equals(labelName)) {
                        metricName = labelValue;
                    } else if (DATA_STREAM_DATASET_LABEL.equals(labelName)) {
                        dataset = DataStream.sanitizeDataset(labelValue);
                    } else if (DATA_STREAM_NAMESPACE_LABEL.equals(labelName)) {
                        namespace = DataStream.sanitizeNamespace(labelValue);
                    }
                }
                if (metricName == null) {
                    droppedSamplesMissingName += seriesSamples;
                    droppedExemplarsMissingName += seriesExemplars;
                    continue;
                }

                for (Sample sample : timeSeries.getSamplesList()) {
                    if (Double.isFinite(sample.getValue()) == false) {
                        continue;
                    }
                    IndexRequest indexRequest = buildIndexRequest(timeSeries, sample, metricName, dataset, namespace);
                    // Guard against label fan-out: the same labels are copied into every per-sample document.
                    if (expandedContentTracker.add(indexRequest.ramBytesUsed(), listener)) {
                        return;
                    }
                    sampleRequests.add(indexRequest);
                }
                if (exemplarIngestionEnabled) {
                    for (Exemplar exemplar : timeSeries.getExemplarsList()) {
                        long exemplarTimestamp = exemplar.getTimestamp() == 0 ? requestTimestamp : exemplar.getTimestamp();
                        if (exemplarTimestamps.add(exemplarTimestamp) < 0) {
                            duplicateExemplars++;
                            continue;
                        }
                        IndexRequest indexRequest = buildExemplarIndexRequest(timeSeries, exemplar, dataset, namespace, exemplarTimestamp);
                        if (expandedContentTracker.add(indexRequest.ramBytesUsed(), listener)) {
                            return;
                        }
                        exemplarRequests.add(indexRequest);
                    }
                }
            }

            for (IndexRequest sampleRequest : sampleRequests) {
                bulkRequestBuilder.add(sampleRequest);
            }
            int firstExemplarDocumentPosition = sampleRequests.size();
            for (IndexRequest exemplarRequest : exemplarRequests) {
                bulkRequestBuilder.add(exemplarRequest);
            }

            logger.debug(
                "Received Prometheus remote write request with {} timeseries, {} samples, and {} exemplars ({} duplicates dropped)",
                writeRequest.getTimeseriesCount(),
                totalSamples,
                totalExemplars,
                duplicateExemplars
            );

            if (totalSamples + totalExemplars == 0) {
                listener.onResponse(new RemoteWriteResponse());
                return;
            }

            if (bulkRequestBuilder.numberOfActions() == 0) {
                if (droppedSamplesMissingName > 0 || droppedExemplarsMissingName > 0) {
                    String message = buildFailureSummary(
                        totalSamples,
                        totalExemplars,
                        droppedSamplesMissingName,
                        droppedExemplarsMissingName,
                        droppedSamplesMissingName,
                        droppedExemplarsMissingName,
                        Map.of()
                    );
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST));
                } else {
                    // No indexable data points remain, which is not a client error.
                    listener.onResponse(new RemoteWriteResponse());
                }
                return;
            }

            final int finalTotalSamples = totalSamples;
            final int finalTotalExemplars = totalExemplars;
            final int finalDroppedSamplesMissingName = droppedSamplesMissingName;
            final int finalDroppedExemplarsMissingName = droppedExemplarsMissingName;
            bulkRequestBuilder.execute(listener.delegateFailure((delegate, bulkResponse) -> {
                if (bulkResponse.hasFailures() || finalDroppedSamplesMissingName > 0 || finalDroppedExemplarsMissingName > 0) {
                    delegate.onFailure(
                        buildPartialFailureException(
                            bulkResponse,
                            finalTotalSamples,
                            finalTotalExemplars,
                            finalDroppedSamplesMissingName,
                            finalDroppedExemplarsMissingName,
                            firstExemplarDocumentPosition
                        )
                    );
                } else {
                    delegate.onResponse(new RemoteWriteResponse());
                }
            }));

        } catch (InvalidProtocolBufferException e) {
            logger.debug("invalid Prometheus remote write payload", e);
            listener.onFailure(
                new ElasticsearchStatusException("Invalid Prometheus remote write payload: " + e.getMessage(), RestStatus.BAD_REQUEST, e)
            );
        } catch (Exception e) {
            logger.error("failed to execute prometheus remote write request", e);
            listener.onFailure(e);
        }
    }

    private static IndexRequest buildExemplarIndexRequest(
        TimeSeries timeSeries,
        Exemplar exemplar,
        String dataset,
        String namespace,
        long timestamp
    ) throws IOException {
        try (XContentBuilder builder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            builder.startObject();

            builder.field("@timestamp", timestamp);

            String fullDataset = dataset + PROMETHEUS_DATASET_SUFFIX;
            builder.startObject("data_stream");
            builder.field("type", "exemplars");
            builder.field("dataset", fullDataset);
            builder.field("namespace", namespace);
            builder.endObject();

            builder.startObject("labels");
            for (Label label : timeSeries.getLabelsList()) {
                if (isIgnoredLabel(label.getName()) == false) {
                    builder.field(label.getName(), label.getValue());
                }
            }
            builder.endObject();

            if (exemplar.getLabelsCount() > 0) {
                builder.startObject("exemplar_labels");
                for (Label label : exemplar.getLabelsList()) {
                    builder.field(label.getName(), label.getValue());
                }
                builder.endObject();
            }

            builder.field("value", exemplar.getValue());
            builder.endObject();

            String targetIndex = EXEMPLARS_DATA_STREAM_PREFIX + dataset + PROMETHEUS_DATASET_SUFFIX + "-" + namespace;
            return new IndexRequest(targetIndex).opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(builder);
        }
    }

    /*
     * Routing control labels are not stored, similar to OTLP TargetIndex attributes
     */
    private static boolean isIgnoredLabel(String labelName) {
        return DATA_STREAM_DATASET_LABEL.equals(labelName) || DATA_STREAM_NAMESPACE_LABEL.equals(labelName);
    }

    private static IndexRequest buildIndexRequest(TimeSeries timeSeries, Sample sample, String metricName, String dataset, String namespace)
        throws IOException {
        try (XContentBuilder builder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            builder.startObject();

            // @timestamp - Prometheus timestamps are in milliseconds
            builder.field("@timestamp", sample.getTimestamp());

            // data_stream fields
            String fullDataset = dataset + PROMETHEUS_DATASET_SUFFIX;
            builder.startObject("data_stream");
            builder.field("type", "metrics");
            builder.field("dataset", fullDataset);
            builder.field("namespace", namespace);
            builder.endObject();

            // labels - all labels including __name__
            builder.startObject("labels");
            for (Label label : timeSeries.getLabelsList()) {
                if (isIgnoredLabel(label.getName()) == false) {
                    builder.field(label.getName(), label.getValue());
                }
            }
            builder.endObject();
            builder.startObject("metrics");

            // metric value - field named after the metric
            builder.field(metricName, sample.getValue());

            builder.endObject();
            builder.endObject();

            String targetIndex = METRICS_DATA_STREAM_PREFIX + fullDataset + "-" + namespace;
            return new IndexRequest(targetIndex).opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(builder);
        }
    }

    private static ElasticsearchStatusException buildPartialFailureException(
        BulkResponse bulkResponse,
        int totalSamples,
        int totalExemplars,
        int droppedSamplesMissingName,
        int droppedExemplarsMissingName,
        int firstExemplarDocumentPosition
    ) {
        Map<String, Map<RestStatus, FailureGroup>> failureGroups = null;
        // Default to 400 per the remote write spec for requests that should not be retried.
        RestStatus responseStatus = RestStatus.BAD_REQUEST;
        int sampleFailures = droppedSamplesMissingName;
        int exemplarFailures = droppedExemplarsMissingName;

        BulkItemResponse[] items = bulkResponse.getItems();
        for (int i = 0; i < items.length; i++) {
            BulkItemResponse item = items[i];
            BulkItemResponse.Failure failure = item.getFailure();
            if (failure != null) {
                if (i < firstExemplarDocumentPosition) {
                    sampleFailures++;
                } else {
                    exemplarFailures++;
                }
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

        String message = buildFailureSummary(
            totalSamples,
            totalExemplars,
            droppedSamplesMissingName,
            droppedExemplarsMissingName,
            sampleFailures,
            exemplarFailures,
            failureGroups
        );
        return new ElasticsearchStatusException(message, responseStatus);
    }

    private static String buildFailureSummary(
        int totalSamples,
        int totalExemplars,
        int droppedSamplesMissingName,
        int droppedExemplarsMissingName,
        int sampleFailures,
        int exemplarFailures,
        @Nullable Map<String, Map<RestStatus, FailureGroup>> failureGroups
    ) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Prometheus remote write request partially failed: ");
        if (sampleFailures > 0) {
            failureMessage.append(sampleFailures).append(" of ").append(totalSamples).append(" samples");
        }
        if (exemplarFailures > 0) {
            if (sampleFailures > 0) {
                failureMessage.append(" and ");
            }
            failureMessage.append(exemplarFailures).append(" of ").append(totalExemplars).append(" exemplars");
        }
        failureMessage.append(" failed.\n");
        if (droppedSamplesMissingName > 0) {
            failureMessage.append(droppedSamplesMissingName).append(" sample(s) dropped due to missing __name__ label\n");
        }
        if (droppedExemplarsMissingName > 0) {
            failureMessage.append(droppedExemplarsMissingName).append(" exemplar(s) dropped due to missing __name__ label\n");
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

    private static class ExpandedContentTracker {
        private final long limit;
        private long total;

        private ExpandedContentTracker(long limit) {
            this.limit = limit;
        }

        private boolean add(long bytes, ActionListener<?> listener) {
            if (bytes <= limit - total) {
                total += bytes;
                return false;
            }
            ElasticsearchStatusException e = new ElasticsearchStatusException(
                "Prometheus remote write request rejected: expanded content would exceed limit [" + limit + "] bytes",
                RestStatus.REQUEST_ENTITY_TOO_LARGE
            );
            logger.debug("failed to execute prometheus remote write request", e);
            listener.onFailure(e);
            return true;
        }
    }

    public static class RemoteWriteRequest extends ActionRequest implements CompositeIndicesRequest, Releasable {
        final ReleasableBytesReference remoteWriteRequest;
        final String dataset;
        final String namespace;
        private final Releasable releaseBody;
        private final Releasable releasePressure;

        public RemoteWriteRequest(
            ReleasableBytesReference remoteWriteRequest,
            String dataset,
            String namespace,
            Releasable indexingPressureRelease
        ) {
            this.remoteWriteRequest = remoteWriteRequest;
            this.dataset = dataset;
            this.namespace = namespace;
            this.releaseBody = Releasables.releaseOnce(remoteWriteRequest);
            this.releasePressure = Releasables.releaseOnce(indexingPressureRelease);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        void releaseBody() {
            releaseBody.close();
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }

        @Override
        public void close() {
            releaseBody.close();
            releasePressure.close();
        }
    }

    public static class RemoteWriteResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }
}
