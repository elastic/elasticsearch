/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.hash4j.internal.ByteArrayUtil;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
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
import org.elasticsearch.common.Strings;
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

public class MetricsDBTransportAction extends HandledTransportAction<
    MetricsDBTransportAction.MetricsRequest,
    MetricsDBTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsDBTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsDBTransportAction.class);
    private static final int MAX_TSID_CACHE_SIZE = 1_000_000;
    private static final Hasher128 HASHER = Hashing.murmur3_128();
    /**
     * Caches tsids for which we've already ingested a time series document.
     * Only relevant if normalized is true.
     */
    private final Set<Tsid> tsidCache = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;


    @Inject
    public MetricsDBTransportAction(
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
            Map<Tsid, TimeSeries> timeSeries = new HashMap<>();

            // We receive a batch so there will be multiple documents as a result of processing it.
            List<LuceneDocument> dataPointDocuments;
            if (request.noop) {
                dataPointDocuments = List.of();
            } else {
                dataPointDocuments = createDataPointDocuments(metricsServiceRequest, request.normalized, timeSeries);
            }
            logger.info("Indexing {} data point documents", dataPointDocuments.size());

            // This loop is similar to TransportShardBulkAction, we fork the processing of the entire request
            // to ThreadPool.Names.WRITE but we perform all writes on the same thread.
            // We expect concurrency to come from a client submitting concurrent requests.
            List<String> errors = new ArrayList<>();
            Engine.IndexResult indexed = engine.index(getIndexRequest(dataPointDocuments, null));
            if (indexed.getFailure() != null) {
                errors.add(indexed.getFailure().getMessage());
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
                    Engine.IndexResult result = engine.index(
                        getIndexRequest(Collections.singletonList(ts.toLuceneDocument()), ts.tsid().toString())
                    );
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

    private static Engine.Index getIndexRequest(List<LuceneDocument> luceneDocuments, @Nullable String id) {
        long autoGeneratedIdTimestamp = -1;
        if (id == null) {
            autoGeneratedIdTimestamp = System.currentTimeMillis();
            id = UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.empty());
        }
        var parsedDocument = new ParsedDocument(
            // Even though this version field is here, it is not added to the LuceneDocument and won't be stored.
            // This is just the contract that the code expects.
            new NumericDocValuesField(NAME, -1L),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            id,
            null,
            luceneDocuments,
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
            autoGeneratedIdTimestamp,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            1
        );
    }

    private List<LuceneDocument> createDataPointDocuments(
        ExportMetricsServiceRequest exportMetricsServiceRequest,
        boolean normalized,
        Map<Tsid, TimeSeries> timeSeries
    ) {
        List<LuceneDocument> documents = new ArrayList<>();
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            HashValue128 resourceAttributesHash = HASHER.hashTo128Bits(
                resourceMetrics.getResource().getAttributesList(),
                AttributeListHashFunnel.get()
            );
            List<IndexableField> resourceAttributes = getAttributeFields(
                "resource.attributes.",
                resourceMetrics.getResource().getAttributesList()
            );
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                HashValue128 scopeAttributesHash = HASHER.hashTo128Bits(
                    scopeMetrics.getScope().getAttributesList(),
                    AttributeListHashFunnel.get()
                );
                List<IndexableField> scopeAttributes = getAttributeFields("scope.attributes.", scopeMetrics.getScope().getAttributesList());
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
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
        HashValue128 resourceAttributesHash,
        List<IndexableField> scopeAttributes,
        HashValue128 scopeAttributesHash,
        Metric metric,
        List<NumberDataPoint> dataPoints,
        Map<Tsid, TimeSeries> timeSeries,
        @Nullable AggregationTemporality temporality,
        boolean normalized) {
        List<LuceneDocument> documents = new ArrayList<>(dataPoints.size());
        HashStream128 hashStream = HASHER.hashStream();
        for (int i = 0; i < dataPoints.size(); i++) {
            NumberDataPoint dp = dataPoints.get(i);
            var doc = new LuceneDocument();
            documents.add(doc);
            doc.add(SortedNumericDocValuesField.indexedField("@timestamp", dp.getTimeUnixNano() / 1_000_000));

            switch (dp.getValueCase()) {
                case AS_DOUBLE -> doc.add(new NumericDocValuesField("value_double", NumericUtils.doubleToSortableLong(dp.getAsDouble())));
                case AS_INT -> doc.add(new NumericDocValuesField("value_long", dp.getAsInt()));
            }
            TimeSeries ts = getOrCreateTimeSeries(
                hashStream,
                timeSeries,
                metric,
                temporality,
                resourceAttributes,
                resourceAttributesHash,
                scopeAttributes,
                scopeAttributesHash,
                dp
            );
            if (normalized) {
                doc.add(ts.tsidField());
            } else {
                ts.addFields(doc);
            }
        }
        return documents;
    }

    private static List<IndexableField> getAttributeFields(String prefix, List<KeyValue> attributes) {
        List<IndexableField> fields = new ArrayList<>(attributes.size());
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue attribute = attributes.get(i);
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
            case BOOL_VALUE -> fields.add(SortedNumericDocValuesField.indexedField(fieldName, value.getBoolValue() ? 1L : 0L));
            case BYTES_VALUE -> fields.add(
                SortedSetDocValuesField.indexedField(fieldName, new BytesRef(value.getBytesValue().toByteArray()))
            );
            case INT_VALUE -> fields.add(SortedNumericDocValuesField.indexedField(fieldName, value.getIntValue()));
            case DOUBLE_VALUE -> fields.add(
                SortedNumericDocValuesField.indexedField(fieldName, NumericUtils.doubleToSortableLong(value.getDoubleValue()))
            );
            case ARRAY_VALUE -> {
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0; i < valuesList.size(); i++) {
                    addField(fields, fieldName, valuesList.get(i));
                }
            }
            default -> throw new IllegalArgumentException("Unsupported attribute type: " + value.getValueCase());
        }
    }

    private static TimeSeries getOrCreateTimeSeries(
        HashStream128 hashStream,
        Map<Tsid, TimeSeries> ts,
        Metric metric,
        @Nullable AggregationTemporality temporality,
        List<IndexableField> resourceAttributes,
        HashValue128 resourceAttributesHash,
        List<IndexableField> scopeAttributes,
        HashValue128 scopeAttributesHash,
        NumberDataPoint dp
    ) {
        hashStream.reset();
        int metricNameHash = hashStream.putString(metric.getName()).getAsInt();
        HashValue128 hashOfAll = hashStream
            .put(dp.getAttributesList(), AttributeListHashFunnel.get())
            .putInt(metric.getDataCase().getNumber())
            .putInt(metric.getUnit().hashCode())
            .putInt(temporality != null ? temporality.getNumber() : -1)
            .putLong(resourceAttributesHash.getLeastSignificantBits())
            .putLong(resourceAttributesHash.getMostSignificantBits())
            .putLong(scopeAttributesHash.getLeastSignificantBits())
            .putLong(scopeAttributesHash.getMostSignificantBits())
            .get();
        hashStream.reset();
        Tsid tsid = new Tsid(metricNameHash, hashOfAll.getLeastSignificantBits(), hashOfAll.getMostSignificantBits());
        return ts.computeIfAbsent(tsid, id -> new TimeSeries(
            id,
            id.toString(),
            metric,
            temporality,
            resourceAttributes,
            scopeAttributes,
            dp
        ));
    }

    record Tsid(int metricNameHash, long leastSignificantBits, long mostSignificantBits) {

        @Override
        @SuppressWarnings("checkstyle:EqualsHashCode")
        public int hashCode() {
            return (int) leastSignificantBits;
        }

        @Override
        public String toString() {
            byte[] bytes = new byte[20];
            ByteArrayUtil.setInt(bytes, 0, metricNameHash);
            ByteArrayUtil.setLong(bytes, 4, leastSignificantBits);
            ByteArrayUtil.setLong(bytes, 12, mostSignificantBits);
            return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
        }
    }

    record TimeSeries(
        Tsid tsid,
        String tsidString,
        Metric metric,
        @Nullable AggregationTemporality temporality,
        List<IndexableField> resourceAttributes,
        List<IndexableField> scopeAttributes,
        NumberDataPoint dp
    ) {
        public void addFields(LuceneDocument doc) {
            doc.add(tsidField());
            doc.add(SortedDocValuesField.indexedField("metric_name", new BytesRef(metric.getName())));
            doc.add(SortedDocValuesField.indexedField("unit", new BytesRef(metric.getUnit())));
            if (temporality != null) {
                doc.add(SortedDocValuesField.indexedField("temporality", new BytesRef(temporality.toString())));
            }
            doc.addAll(resourceAttributes);
            doc.addAll(scopeAttributes);
            doc.addAll(getAttributeFields("attributes.", dp.getAttributesList()));
        }

        public LuceneDocument toLuceneDocument() {
            var doc = new LuceneDocument();
            doc.add(new StringField(NAME, tsidString, Field.Store.YES));
            doc.add(new NumericDocValuesField("_version", 1));
            doc.add(new NumericDocValuesField("_seq_no", 1));
            doc.add(new NumericDocValuesField("_primary_term", 0));
            addFields(doc);
            return doc;
        }

        public IndexableField tsidField() {
            return SortedDocValuesField.indexedField("_tsid", new BytesRef(tsidString));
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
    }
}
