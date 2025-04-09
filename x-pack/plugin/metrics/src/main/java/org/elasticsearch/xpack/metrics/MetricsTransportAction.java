/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import com.google.protobuf.MessageLite;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsTransportAction extends HandledTransportAction<
    MetricsTransportAction.MetricsRequest,
    MetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsTransportAction.class);
    private static final int MAX_TSID_CACHE_SIZE = 1_000_000;
    /**
     * Caches tsids for which we've already ingested a time series document.
     * Only relevant if normalized is true.
     */
    private final Set<String> tsidCache = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;


    @Inject
    public MetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndicesService indicesService
    ) {
        super(NAME, transportService, actionFilters, MetricsRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, MetricsRequest request, ActionListener<MetricsResponse> listener) {
        try {
            var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.exportMetricsServiceRequest.streamInput());

            final ClusterState clusterState = clusterService.state();
            final ProjectMetadata project = projectResolver.getProjectMetadata(clusterState);
            var indexAbstraction = project.getIndicesLookup().get("metricsdb");
            if (indexAbstraction == null) {
                throw new IllegalStateException("Index [metricsdb] does not exist");
            }
            var writeIndex = indexAbstraction.getWriteIndex();
            var indexService = indicesService.indexServiceSafe(writeIndex);

            // TODO proper routing ???
            var shard = indexService.getShard(0);
            var engine = shard.getEngineOrNull();
            Map<String, TimeSeries> timeSeries = new HashMap<>();

            // We receive a batch so there will be multiple documents as a result of processing it.
            var dataPointDocuments = createDataPointDocuments(metricsServiceRequest, request.normalized, timeSeries);
            logger.info("Indexing {} data point documents", dataPointDocuments.size());

            // This loop is similar to TransportShardBulkAction, we fork the processing of the entire request
            // to ThreadPool.Names.WRITE but we perform all writes on the same thread.
            // We expect concurrency to come from a client submitting concurrent requests.
            List<String> errors = new ArrayList<>();
            for (var luceneDocument : dataPointDocuments) {
                Engine.IndexResult indexed = engine.index(getIndexRequest(luceneDocument, null));
                if (indexed.getFailure() != null) {
                    errors.add(indexed.getFailure().getMessage());
                }
            }

            if (request.normalized) {
                int indexedTimeSeries = 0;
                for (TimeSeries ts : timeSeries.values()) {
                    if (tsidCache.contains(ts.tsid())) {
                        continue;
                    }
                    if (tsidCache.size() > MAX_TSID_CACHE_SIZE) {
                        tsidCache.clear();
                    }
                    tsidCache.add(ts.tsid());
                    Engine.IndexResult result = engine.index(getIndexRequest(ts.toLuceneDocument(), ts.tsid()));
                    if (result.getFailure() != null) {
                        errors.add(result.getFailure().getMessage());
                    } else {
                        indexedTimeSeries++;
                    }
                }
                if (indexedTimeSeries > 0) {
                    logger.info("Indexed [{}] time-series docs", indexedTimeSeries);
                }
            }

            // TODO make sure to enable periodic flush on the index (?)
            if (errors.isEmpty() == false) {
                logger.warn("Indexing errors. First error: [{}]", errors.getFirst());
                listener.onResponse(
                    new MetricsResponse(
                        ExportMetricsServiceResponse.newBuilder()
                            .getPartialSuccessBuilder()
                            .setRejectedDataPoints(errors.size())
                            .setErrorMessage(String.join("\n", errors))
                            .build()
                    )
                );
            } else {
                listener.onResponse(new MetricsResponse(ExportMetricsServiceResponse.newBuilder().build()));
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private static Engine.Index getIndexRequest(LuceneDocument luceneDocument, @Nullable String id) {
        if (id == null) {
            id = UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.empty());
        }
        var parsedDocument = new ParsedDocument(
            // Even though this version field is here, it is not added to the LuceneDocument and won't be stored.
            // This is just the contract that the code expects.
            new NumericDocValuesField(NAME, -1L),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            id,
            null,
            List.of(luceneDocument),
            null,
            XContentType.JSON,
            null,
            0
        );
        return new Engine.Index(
            Uid.encodeId(id),
            parsedDocument,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            1,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            1
        );
    }

    private List<LuceneDocument> createDataPointDocuments(
        ExportMetricsServiceRequest exportMetricsServiceRequest,
        boolean normalized,
        Map<String, TimeSeries> timeSeries
    ) {
        List<LuceneDocument> documents = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : exportMetricsServiceRequest.getResourceMetricsList()) {
            int resourceAttributesHash = resourceMetrics.getResource().getAttributesList().hashCode();
            List<IndexableField> resourceAttributes = getAttributeFields(
                "resource.attributes.",
                resourceMetrics.getResource().getAttributesList()
            );
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                int scopeAttributesHash = scopeMetrics.getScope().getAttributesList().hashCode();
                List<IndexableField> scopeAttributes = getAttributeFields("scope.attributes.", scopeMetrics.getScope().getAttributesList());
                for (var metric : scopeMetrics.getMetricsList()) {
                    switch (metric.getDataCase()) {
                        case SUM -> documents.addAll(
                            createNumberDataPointDocs(
                                resourceAttributes,
                                resourceAttributesHash,
                                scopeAttributes,
                                scopeAttributesHash,
                                metric,
                                metric.getSum().getDataPointsList(),
                                timeSeries,
                                metric.getSum().getAggregationTemporality(),
                                normalized
                            )
                        );
                        case GAUGE -> documents.addAll(
                            createNumberDataPointDocs(
                                resourceAttributes,
                                resourceAttributesHash,
                                scopeAttributes,
                                scopeAttributesHash,
                                metric,
                                metric.getGauge().getDataPointsList(),
                                timeSeries,
                                null,
                                normalized
                            )
                        );
                        default -> throw new IllegalArgumentException("Unsupported metric type [" + metric.getDataCase() + "]");
                    }
                }
            }
        }
        return documents;
    }

    private static List<LuceneDocument> createNumberDataPointDocs(
        List<IndexableField> resourceAttributes,
        int resourceAttributesHash,
        List<IndexableField> scopeAttributes,
        int scopeAttributesHash,
        Metric metric,
        List<NumberDataPoint> dataPoints,
        Map<String, TimeSeries> timeSeries,
        @Nullable AggregationTemporality temporality,
        boolean normalized) {
        List<LuceneDocument> documents = new ArrayList<>(dataPoints.size());
        for (NumberDataPoint dp : dataPoints) {
            var doc = new LuceneDocument();
            documents.add(doc);
            doc.add(SortedNumericDocValuesField.indexedField("@timestamp", dp.getTimeUnixNano() / 1_000_000));

            switch (dp.getValueCase()) {
                case AS_DOUBLE -> doc.add(new NumericDocValuesField("value_double", NumericUtils.doubleToSortableLong(dp.getAsDouble())));
                case AS_INT -> doc.add(new NumericDocValuesField("value_long", dp.getAsInt()));
            }
            TimeSeries ts = getOrCreateTimeSeries(
                timeSeries,
                metric,
                temporality,
                resourceAttributes,
                resourceAttributesHash,
                scopeAttributes,
                scopeAttributesHash,
                dp
            );
            if (normalized == false) {
                ts.addFields(doc);
            } else {
                doc.add(ts.tsidField());
            }
        }
        return documents;
    }

    private static List<IndexableField> getAttributeFields(String prefix, List<KeyValue> attributes) {
        List<IndexableField> fields = new ArrayList<>(attributes.size());
        for (KeyValue attribute : attributes) {
            String name = attribute.getKey();
            if (name.isEmpty()) {
                continue;
            }
            addField(fields, prefix + name, attribute.getValue());
        }
        return fields;
    }

    private static void addField(List<IndexableField> fields, String fieldName, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> fields.add(SortedSetDocValuesField.indexedField(fieldName, new BytesRef(value.getStringValue())));
            case BOOL_VALUE -> fields.add(
                SortedNumericDocValuesField.indexedField(fieldName, value.getBoolValue() ? 1L : 0L)
            );
            case BYTES_VALUE -> fields.add(
                SortedSetDocValuesField.indexedField(fieldName, new BytesRef(value.getBytesValue().toByteArray()))
            );
            case INT_VALUE -> fields.add(SortedNumericDocValuesField.indexedField(fieldName, value.getIntValue()));
            case DOUBLE_VALUE -> fields.add(
                SortedNumericDocValuesField.indexedField(fieldName, NumericUtils.doubleToSortableLong(value.getDoubleValue()))
            );
            case ARRAY_VALUE -> {
                for (AnyValue arrayValue : value.getArrayValue().getValuesList()) {
                    addField(fields, fieldName, arrayValue);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported attribute type: " + value.getValueCase());
        }
    }

    private static TimeSeries getOrCreateTimeSeries(
        Map<String, TimeSeries> ts,
        Metric metric,
        @Nullable AggregationTemporality temporality,
        List<IndexableField> resourceAttributes,
        int resourceAttributesHash,
        List<IndexableField> scopeAttributes,
        int scopeAttributesHash,
        NumberDataPoint dp
    ) {
        String tsid = ""
                      + resourceAttributesHash
                      + scopeAttributesHash
                      + metric.getName()
                      + metric.getUnit()
                      + metric.getDataCase()
                      + dp.getAttributesList().hashCode();
        if (temporality != null) {
            tsid += temporality.getNumber();
        }
        if (ts.containsKey(tsid)) {
            return ts.get(tsid);
        } else {
            SortedDocValuesField tsidField = SortedDocValuesField.indexedField("_tsid", new BytesRef(tsid));
            SortedDocValuesField metricNameField = SortedDocValuesField.indexedField("metric_name", new BytesRef(metric.getName()));
            SortedDocValuesField unitField = SortedDocValuesField.indexedField("unit", new BytesRef(metric.getUnit()));
            SortedDocValuesField temporalityField = null;
            if (temporality != null) {
                temporalityField = SortedDocValuesField.indexedField("temporality", new BytesRef(temporality.toString()));
            }
            List<IndexableField> dpAttributes = getAttributeFields("attributes.", dp.getAttributesList());
            TimeSeries timeSeries = new TimeSeries(
                tsid,
                tsidField,
                metricNameField,
                unitField,
                temporalityField,
                resourceAttributes,
                scopeAttributes,
                dpAttributes
            );
            ts.put(tsid, timeSeries);
            return timeSeries;
        }
    }

    record TimeSeries(
        String tsid,
        IndexableField tsidField,
        IndexableField metricName,
        IndexableField unit,
        @Nullable IndexableField temporality,
        List<IndexableField> resourceAttributes,
        List<IndexableField> scopeAttributes,
        List<IndexableField> dpAttributes
    ) {
        public void addFields(LuceneDocument doc) {
            doc.add(tsidField);
            doc.add(metricName);
            if (temporality != null) {
                doc.add(temporality);
            }
            doc.addAll(resourceAttributes);
            doc.addAll(scopeAttributes);
            doc.addAll(dpAttributes);
        }

        public LuceneDocument toLuceneDocument() {
            var doc = new LuceneDocument();
            addFields(doc);
            return doc;
        }
    }

    public static class MetricsRequest extends ActionRequest {
        private final boolean normalized;
        private final BytesReference exportMetricsServiceRequest;

        public MetricsRequest(StreamInput in) throws IOException {
            super(in);
            normalized = in.readBoolean();
            exportMetricsServiceRequest = in.readBytesReference();
        }

        public MetricsRequest(boolean normalized, BytesReference exportMetricsServiceRequest) {
            this.normalized = normalized;
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
    }
}
