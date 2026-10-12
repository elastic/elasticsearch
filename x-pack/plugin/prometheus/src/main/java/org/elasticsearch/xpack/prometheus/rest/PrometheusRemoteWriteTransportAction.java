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
    private final long maxExpandedContentLength;

    @Inject
    public PrometheusRemoteWriteTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        Settings settings
    ) {
        super(NAME, transportService, actionFilters, in -> TransportAction.localOnly(), threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
        this.threadPool = threadPool;
        this.maxExpandedContentLength = HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.get(settings).getBytes();
    }

    @Override
    protected void doExecute(Task task, RemoteWriteRequest request, ActionListener<RemoteWriteResponse> listener) {
        try (request) {
            RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.parseFrom(request.remoteWriteRequest.streamInput());
            request.releaseBody();

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            boolean exemplarIngestionEnabled = PrometheusPlugin.METRIC_EXEMPLARS_FEATURE_FLAG.isEnabled();
            // Exemplars without a timestamp are stamped with the time the request was received. A re-sent request (e.g. after a
            // 429) therefore assigns a new timestamp to such exemplars, so they are indexed again instead of being rejected as
            // duplicates. We accept this limitation.
            long requestTimestamp = exemplarIngestionEnabled ? threadPool.absoluteTimeInMillis() : 0;

            int totalSamples = 0;
            int droppedSamplesMissingName = 0;
            ExemplarCounters exemplarCounters = new ExemplarCounters();
            ExpandedContentTracker expandedContentTracker = new ExpandedContentTracker(maxExpandedContentLength);
            List<IndexRequest> sampleRequests = new ArrayList<>();
            List<IndexRequest> exemplarRequests = new ArrayList<>();
            for (TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                int seriesSamples = timeSeries.getSamplesCount();
                int seriesExemplars = exemplarIngestionEnabled ? timeSeries.getExemplarsCount() : 0;
                totalSamples += seriesSamples;
                exemplarCounters.total += seriesExemplars;

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
                    exemplarCounters.droppedMissingName += seriesExemplars;
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
                        if (Double.isFinite(exemplar.getValue()) == false) {
                            // Prometheus passes non-finite exemplar values through, but the double mapping cannot store them.
                            exemplarCounters.droppedNonFinite++;
                            continue;
                        }
                        // Assigning the request timestamp can cause duplicate timestamps if a series has multiple exemplars
                        // without a timestamp. We leave the duplicate detection to the indexing request.
                        long exemplarTimestamp = exemplar.getTimestamp() == 0 ? requestTimestamp : exemplar.getTimestamp();
                        IndexRequest indexRequest = buildExemplarIndexRequest(timeSeries, exemplar, dataset, namespace, exemplarTimestamp);
                        if (expandedContentTracker.add(indexRequest.ramBytesUsed(), listener)) {
                            return;
                        }
                        exemplarRequests.add(indexRequest);
                    }
                }
            }

            // Samples are added before exemplars so that bulk item failures can be attributed to either of them by position.
            for (IndexRequest sampleRequest : sampleRequests) {
                bulkRequestBuilder.add(sampleRequest);
            }
            int firstExemplarDocumentPosition = sampleRequests.size();
            for (IndexRequest exemplarRequest : exemplarRequests) {
                bulkRequestBuilder.add(exemplarRequest);
            }

            logger.debug(
                "Received Prometheus remote write request with {} timeseries, {} samples, and {} exemplars",
                writeRequest.getTimeseriesCount(),
                totalSamples,
                exemplarCounters.total
            );

            if (totalSamples + exemplarCounters.total == 0) {
                listener.onResponse(new RemoteWriteResponse());
                return;
            }

            if (bulkRequestBuilder.numberOfActions() == 0) {
                logExemplarProblems(exemplarCounters, null);
                if (droppedSamplesMissingName > 0) {
                    String message = buildFailureSummary(totalSamples, droppedSamplesMissingName, droppedSamplesMissingName, null);
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST));
                } else {
                    // No indexable data points remain, which is not a client error.
                    listener.onResponse(new RemoteWriteResponse());
                }
                return;
            }

            final int finalTotalSamples = totalSamples;
            final int finalDroppedSamplesMissingName = droppedSamplesMissingName;
            bulkRequestBuilder.execute(
                listener.delegateFailure(
                    (delegate, bulkResponse) -> handleBulkResponse(
                        bulkResponse,
                        finalTotalSamples,
                        finalDroppedSamplesMissingName,
                        exemplarCounters,
                        firstExemplarDocumentPosition,
                        delegate
                    )
                )
            );

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

            writeSeriesLabels(builder, timeSeries);

            boolean hasExemplarLabels = false;
            for (Label label : exemplar.getLabelsList()) {
                if (Strings.hasLength(label.getValue()) == false) {
                    continue;
                }
                if (hasExemplarLabels == false) {
                    builder.startObject("exemplar_labels");
                    hasExemplarLabels = true;
                }
                builder.field(label.getName(), label.getValue());
            }
            if (hasExemplarLabels) {
                builder.endObject();
            }

            builder.field("value", exemplar.getValue());
            builder.endObject();

            String targetIndex = EXEMPLARS_DATA_STREAM_PREFIX + fullDataset + "-" + namespace;
            return new IndexRequest(targetIndex).opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(builder);
        }
    }

    /**
     * Writes the series labels (including {@code __name__}) as the {@code labels} object. Sample and exemplar documents must use
     * identical labels because they form the time series dimensions of both data streams. Prometheus treats a label with an empty
     * value as absent, so such labels are skipped.
     */
    private static void writeSeriesLabels(XContentBuilder builder, TimeSeries timeSeries) throws IOException {
        builder.startObject("labels");
        for (Label label : timeSeries.getLabelsList()) {
            if (isIgnoredLabel(label.getName()) == false && Strings.hasLength(label.getValue())) {
                builder.field(label.getName(), label.getValue());
            }
        }
        builder.endObject();
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

            writeSeriesLabels(builder, timeSeries);
            builder.startObject("metrics");

            // metric value - field named after the metric
            builder.field(metricName, sample.getValue());

            builder.endObject();
            builder.endObject();

            String targetIndex = METRICS_DATA_STREAM_PREFIX + fullDataset + "-" + namespace;
            return new IndexRequest(targetIndex).opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(builder);
        }
    }

    /**
     * Only sample failures decide whether the request fails. Exemplar failures never affect the response, following upstream
     * Prometheus (which does not fail remote write requests on exemplar ingestion errors) and the OTLP endpoint (which only reports
     * them as a warning). Remote write has no partial success channel, so exemplar problems are only logged.
     */
    private static void handleBulkResponse(
        BulkResponse bulkResponse,
        int totalSamples,
        int droppedSamplesMissingName,
        ExemplarCounters exemplarCounters,
        int firstExemplarDocumentPosition,
        ActionListener<RemoteWriteResponse> listener
    ) {
        Map<String, Map<RestStatus, FailureGroup>> sampleFailureGroups = null;
        Map<String, Map<RestStatus, FailureGroup>> exemplarFailureGroups = null;
        // Default to 400 per the remote write spec for requests that should not be retried.
        RestStatus responseStatus = RestStatus.BAD_REQUEST;
        int sampleFailures = droppedSamplesMissingName;

        BulkItemResponse[] items = bulkResponse.getItems();
        for (int i = 0; i < items.length; i++) {
            BulkItemResponse.Failure failure = items[i].getFailure();
            if (failure != null) {
                if (i < firstExemplarDocumentPosition) {
                    sampleFailures++;
                    if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                        // 429 takes priority so clients retry (valid samples that were rate-limited may succeed on retry).
                        responseStatus = RestStatus.TOO_MANY_REQUESTS;
                    }
                    sampleFailureGroups = addFailure(sampleFailureGroups, failure);
                } else { // the failure occurred for an exemplar, so not an actual error reported to the user
                    if (failure.getStatus() == RestStatus.CONFLICT) {
                        // Exemplar data streams use time series mode, so an exemplar with the same series labels and timestamp as an
                        // already indexed one (within this request, or from a re-sent batch) is rejected as a version conflict.
                        // The first exemplar wins, which is the intended deduplication.
                        exemplarCounters.duplicates++;
                    } else {
                        exemplarCounters.failed++;
                        exemplarFailureGroups = addFailure(exemplarFailureGroups, failure);
                    }
                }
            }
        }

        logExemplarProblems(exemplarCounters, exemplarFailureGroups);
        if (sampleFailures > 0) {
            String message = buildFailureSummary(totalSamples, sampleFailures, droppedSamplesMissingName, sampleFailureGroups);
            listener.onFailure(new ElasticsearchStatusException(message, responseStatus));
        } else {
            listener.onResponse(new RemoteWriteResponse());
        }
    }

    private static Map<String, Map<RestStatus, FailureGroup>> addFailure(
        @Nullable Map<String, Map<RestStatus, FailureGroup>> failureGroups,
        BulkItemResponse.Failure failure
    ) {
        if (failureGroups == null) {
            failureGroups = new HashMap<>();
        }
        failureGroups.computeIfAbsent(failure.getIndex(), k -> new HashMap<>())
            .computeIfAbsent(failure.getStatus(), k -> new FailureGroup(new AtomicInteger(0), failure.getMessage()))
            .failureCount()
            .incrementAndGet();
        return failureGroups;
    }

    private static void logExemplarProblems(
        ExemplarCounters exemplarCounters,
        @Nullable Map<String, Map<RestStatus, FailureGroup>> exemplarFailureGroups
    ) {
        if (exemplarCounters.hasProblems() && logger.isDebugEnabled()) {
            StringBuilder message = new StringBuilder("Prometheus remote write request: ");
            exemplarCounters.appendSummary(message, exemplarFailureGroups);
            logger.debug(message.toString());
        }
    }

    private static String buildFailureSummary(
        int totalSamples,
        int sampleFailures,
        int droppedSamplesMissingName,
        @Nullable Map<String, Map<RestStatus, FailureGroup>> sampleFailureGroups
    ) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Prometheus remote write request partially failed: ")
            .append(sampleFailures)
            .append(" of ")
            .append(totalSamples)
            .append(" samples failed.\n");
        if (droppedSamplesMissingName > 0) {
            failureMessage.append(droppedSamplesMissingName).append(" sample(s) dropped due to missing __name__ label\n");
        }
        appendFailureGroups(failureMessage, sampleFailureGroups);
        return failureMessage.toString();
    }

    private static void appendFailureGroups(StringBuilder message, @Nullable Map<String, Map<RestStatus, FailureGroup>> failureGroups) {
        if (failureGroups == null) {
            return;
        }
        for (Map.Entry<String, Map<RestStatus, FailureGroup>> indexEntry : failureGroups.entrySet()) {
            for (Map.Entry<RestStatus, FailureGroup> statusEntry : indexEntry.getValue().entrySet()) {
                FailureGroup group = statusEntry.getValue();
                message.append("Index [")
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

    /**
     * Tracks what happened to the exemplars of a request. None of these counts affect the response status.
     */
    private static class ExemplarCounters {
        int total;
        int droppedMissingName;
        int droppedNonFinite;
        int duplicates;
        int failed;

        boolean hasProblems() {
            return droppedMissingName + droppedNonFinite + failed > 0;
        }

        void appendSummary(StringBuilder message, @Nullable Map<String, Map<RestStatus, FailureGroup>> exemplarFailureGroups) {
            message.append(droppedMissingName + droppedNonFinite + failed)
                .append(" of ")
                .append(total)
                .append(" exemplars were not indexed.\n");
            if (droppedMissingName > 0) {
                message.append(droppedMissingName).append(" exemplar(s) dropped due to missing __name__ label\n");
            }
            if (droppedNonFinite > 0) {
                message.append(droppedNonFinite).append(" exemplar(s) dropped due to non-finite value\n");
            }
            appendFailureGroups(message, exemplarFailureGroups);
        }
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
