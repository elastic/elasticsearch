/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
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
import java.util.List;
import java.util.OptionalInt;

public class MetricsTransportAction extends HandledTransportAction<
    MetricsTransportAction.MetricsRequest,
    MetricsTransportAction.MetricsResponse> {

    public static final String NAME = "indices:data/write/metrics";
    public static final ActionType<MetricsTransportAction.MetricsResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(MetricsTransportAction.class);

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

            // We receive a batch so there will be multiple documents as a result of processing it.
            var documents = createLuceneDocuments(metricsServiceRequest, request.normalized);
            logger.info("Received {} metrics", documents.size());

            // This loop is similar to TransportShardBulkAction, we fork the processing of the entire request
            // to ThreadPool.Names.WRITE but we perform all writes on the same thread.
            // We expect concurrency to come from a client submitting concurrent requests.
            for (var luceneDocument : documents) {
                var id = UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.empty());

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
                var indexRequest = new Engine.Index(
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

                engine.index(indexRequest);
            }

            // TODO make sure to enable periodic flush on the index (?)

            listener.onResponse(new MetricsResponse());
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private List<LuceneDocument> createLuceneDocuments(ExportMetricsServiceRequest exportMetricsServiceRequest, boolean normalized) {
        List<LuceneDocument> documents = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : exportMetricsServiceRequest.getResourceMetricsList()) {
            int resourceAttributesHash = resourceMetrics.getResource().getAttributesList().hashCode();
            List<IndexableField> resourceAttributes;
            if (normalized) {
                resourceAttributes = List.of();
            } else {
                resourceAttributes = getAttributeFields("resource.attributes.", resourceMetrics.getResource().getAttributesList());
            }
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                int scopeAttributesHash = scopeMetrics.getScope().getAttributesList().hashCode();
                List<IndexableField> scopeAttributes;
                if (normalized) {
                    scopeAttributes = List.of();
                } else {
                    scopeAttributes = getAttributeFields("scope.attributes.", scopeMetrics.getScope().getAttributesList());
                }
                for (var metric : scopeMetrics.getMetricsList()) {
                    String tsid = ""
                        + resourceAttributesHash
                        + scopeAttributesHash
                        + metric.getName()
                        + metric.getUnit()
                        + metric.getDataCase();

                    switch (metric.getDataCase()) {
                        case SUM -> documents.addAll(
                            createNumberDataPointDocs(
                                resourceAttributes,
                                scopeAttributes,
                                metric.getSum().getDataPointsList(),
                                tsid,
                                metric.getSum().getAggregationTemporality()
                            )
                        );
                        case GAUGE -> documents.addAll(
                            createNumberDataPointDocs(
                                resourceAttributes,
                                scopeAttributes,
                                metric.getGauge().getDataPointsList(),
                                tsid,
                                null
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
        List<IndexableField> scopeAttributes,
        List<NumberDataPoint> dataPoints,
        String tsid,
        @Nullable AggregationTemporality temporality
    ) {
        List<LuceneDocument> documents = new ArrayList<>(dataPoints.size());
        for (NumberDataPoint dp : dataPoints) {
            var doc = new LuceneDocument();
            documents.add(doc);
            doc.add(SortedNumericDocValuesField.indexedField("@timestamp", dp.getTimeUnixNano() / 1_000_000));
            int dpAttributesHash = dp.getAttributesList().hashCode();
            tsid = tsid + dpAttributesHash;
            if (temporality != null) {
                tsid += temporality.getNumber();
            }
            doc.add(SortedDocValuesField.indexedField("_tsid", new BytesRef(tsid)));
            switch (dp.getValueCase()) {
                case AS_DOUBLE -> doc.add(new NumericDocValuesField("value_double", NumericUtils.doubleToSortableLong(dp.getAsDouble())));
                case AS_INT -> doc.add(new NumericDocValuesField("value_long", dp.getAsInt()));
            }
            doc.addAll(resourceAttributes);
            doc.addAll(scopeAttributes);
        }
        return documents;
    }

    private List<IndexableField> getAttributeFields(String prefix, List<KeyValue> attributes) {
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

    private void addField(List<IndexableField> fields, String fieldName, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> fields.add(SortedDocValuesField.indexedField(fieldName, new BytesRef(value.getStringValue())));
            case BOOL_VALUE -> fields.add(
                SortedDocValuesField.indexedField(fieldName, new BytesRef(Boolean.toString(value.getBoolValue())))
            );
            case BYTES_VALUE -> fields.add(SortedDocValuesField.indexedField(fieldName, new BytesRef(value.getBytesValue().toByteArray())));
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
        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
