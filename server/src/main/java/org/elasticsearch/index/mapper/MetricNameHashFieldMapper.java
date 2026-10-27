/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.PresentDocIterator;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mapper for the {@code _metric_name_hash} field of time series indices that sort on it (see
 * {@link IndexSettings#TIME_SERIES_DIMENSION_GROUP_FIELD}).
 * <p>
 * The field is the primary index sort key and clusters the documents of one metric together, which used to be the job of the
 * metric similarity prefix byte of the tsid. Its value is an 8-byte big-endian hash of the metric name(s) of the document:
 * <ul>
 *     <li>for OTel metrics, a hash of {@value #OTEL_METRIC_NAMES_HASH_FIELD}, which the OTLP endpoint sets to a hash over all the
 *     metric names in the document, i.e. the same value for documents that carry the same set of metrics;</li>
 *     <li>for Prometheus metrics, a hash of {@value #PROMETHEUS_METRIC_NAME_FIELD}, the single metric name;</li>
 *     <li>otherwise, a hash over the names of the dimension fields of the document, like the prefix byte did.</li>
 * </ul>
 * The value is derived on the data node from the parsed document, so no extra data travels from the coordinating node.
 */
public class MetricNameHashFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_metric_name_hash";
    /** Set by the OTLP endpoint to a hash over the metric names of the document, and mapped as a dimension. */
    public static final String OTEL_METRIC_NAMES_HASH_FIELD = "_metric_names_hash";
    /** The metric name label of Prometheus metrics. */
    public static final String PROMETHEUS_METRIC_NAME_FIELD = "labels.__name__";
    /** The fields naming the metric(s) of a document, in order of precedence; only used when mapped as dimensions. */
    private static final String[] METRIC_NAME_FIELDS = { OTEL_METRIC_NAMES_HASH_FIELD, PROMETHEUS_METRIC_NAME_FIELD };

    public static final MetricNameHashFieldType FIELD_TYPE = new MetricNameHashFieldType();

    public static final MetricNameHashFieldMapper INSTANCE = new MetricNameHashFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> c.getIndexSettings().useTimeSeriesDimensionGroupSort() ? INSTANCE : null
    );

    public static final class MetricNameHashFieldType extends MappedFieldType {

        private MetricNameHashFieldType() {
            super(NAME, IndexType.docValuesOnly(), false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + NAME + "] is not searchable");
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }
    }

    private MetricNameHashFieldMapper() {
        super(FIELD_TYPE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        LuceneDocument doc = context.rootDoc();
        IndexableField metricName = null;
        for (String metricNameField : METRIC_NAME_FIELDS) {
            // Only a dimension can name the metric: the hash has to be a function of the tsid, see DimensionGroupFieldMapper#isDimension
            if (DimensionGroupFieldMapper.isDimension(context, metricNameField)) {
                metricName = doc.getField(metricNameField);
                if (metricName != null) {
                    break;
                }
            }
        }
        final BytesRef hash;
        if (metricName != null && metricName.binaryValue() != null) {
            BytesRef value = metricName.binaryValue();
            hash = encode(hashValue(value.bytes, value.offset, value.length, new MurmurHash3.Hash128()));
        } else {
            hash = dimensionNamesHash(context);
        }
        if (hash != null) {
            doc.add(new SortedDocValuesField(NAME, hash));
        }
    }

    /**
     * The fallback for documents without a metric name field: a hash over the distinct names of the dimension fields of the
     * document. The dimension fields are the ones matching {@code index.dimensions}, or the ones collected from
     * {@code index.routing_path} while parsing.
     */
    private static BytesRef dimensionNamesHash(DocumentParserContext context) {
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        long sum = 0;
        boolean found = false;
        if (context.indexSettings().getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions) {
            Set<String> seen = new HashSet<>();
            for (IndexableField field : context.rootDoc().getFields()) {
                String name = field.name();
                if (seen.add(name) && forIndexDimensions.matchesField(name)) {
                    sum += pathHash(hasher, name);
                    found = true;
                }
            }
            // A dimension whose value was ignored (e.g. ignore_above) is still a dimension of the document, as it is on the
            // columnar path, which sees the value in the source column
            for (String name : context.getIgnoredFields()) {
                if (seen.add(name) && forIndexDimensions.matchesField(name)) {
                    sum += pathHash(hasher, name);
                    found = true;
                }
            }
        } else if (context.getRoutingFields() instanceof RoutingPathFields routingPathFields) {
            for (BytesRef name : routingPathFields.routingValues().keySet()) {
                sum += pathHash(hasher, name.utf8ToString());
                found = true;
            }
        }
        return found ? encode(sum) : null;
    }

    private static long pathHash(BufferedMurmur3Hasher hasher, String path) {
        hasher.reset();
        hasher.addString(path);
        return hasher.digestHash().h1;
    }

    static long hashValue(byte[] bytes, int offset, int length, MurmurHash3.Hash128 scratch) {
        return MurmurHash3.hash128(bytes, offset, length, 0L, scratch).h1;
    }

    /** Big-endian so that the byte order of the sorted doc values matches the numeric order of the hash. */
    static BytesRef encode(long hash) {
        byte[] bytes = new byte[Long.BYTES];
        ByteUtils.writeLongBE(hash, bytes, 0);
        return new BytesRef(bytes);
    }

    /** Mirrors the {@code new SortedDocValuesField(NAME, ...)} written in {@link #postParse}. */
    private static final IndexableFieldType DV_TYPE = new SortedDocValuesField("", new BytesRef()).fieldType();

    @Override
    protected boolean doSupportsColumnarParse(IndexSettings indexSettings) {
        // The dimension-names fallback needs the index.dimensions predicate; routing_path indices stay on the row path, where
        // the dimension names are collected while parsing.
        return indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions;
    }

    @Override
    public void postColumnarParse(BatchMappingContext context) {
        EscfBatch batch = DimensionGroupFieldMapper.escfBatch(context.sourceBatch());
        SourceSchema schema = batch.schema();
        int docCount = context.docCount();
        BytesRef[] hashes = new BytesRef[docCount];

        // Same precedence as the row path, per row: the OTel field, then the Prometheus field, then the dimension names.
        int remaining = docCount;
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        for (String metricNameField : METRIC_NAME_FIELDS) {
            if (DimensionGroupFieldMapper.isDimension(context.indexSettings(), context.mappingLookup(), metricNameField) == false) {
                continue;
            }
            int metricNameLeaf = DimensionGroupFieldMapper.leafIndexOf(schema, metricNameField);
            if (metricNameLeaf < 0 || remaining == 0) {
                continue;
            }
            BytesRef[] names = DimensionGroupFieldMapper.firstStringValues(batch.column(metricNameLeaf), docCount);
            for (int d = 0; d < docCount; d++) {
                BytesRef name = names[d];
                if (name != null && hashes[d] == null) {
                    hashes[d] = encode(hashValue(name.bytes, name.offset, name.length, scratch));
                    remaining--;
                }
            }
        }
        if (remaining > 0) {
            dimensionNamesHashes(batch, context, hashes);
        }
        context.addColumn(MappedColumns.binaryColumn(hashes, NAME, DV_TYPE));
    }

    /** The columnar counterpart of {@link #dimensionNamesHash}, for the rows that have no hash yet. */
    private static void dimensionNamesHashes(EscfBatch batch, BatchMappingContext context, BytesRef[] hashes) {
        var forIndexDimensions = (IndexRouting.ExtractFromSource.ForIndexDimensions) context.indexSettings().getIndexRouting();
        SourceSchema schema = batch.schema();
        record DimColumn(int leafIdx, String path) {}
        List<DimColumn> dimColumns = new ArrayList<>();
        for (int leafIdx = 0; leafIdx < schema.leafCount(); leafIdx++) {
            String path = schema.getFullPath(leafIdx);
            if (forIndexDimensions.matchesField(path)) {
                dimColumns.add(new DimColumn(leafIdx, path));
            }
        }
        // Two leaf columns can report the same full path (dotted and nested spellings are not merged), so sort by path and count a
        // path once per row, as the row path's distinct-names set does.
        dimColumns.sort(Comparator.comparing(DimColumn::path).thenComparingInt(DimColumn::leafIdx));

        int docCount = hashes.length;
        long[] sums = new long[docCount];
        boolean[] found = new boolean[docCount];
        int[] lastPathGroup = new int[docCount];
        Arrays.fill(lastPathGroup, -1);
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        int pathGroup = -1;
        String previousPath = null;
        for (DimColumn dc : dimColumns) {
            if (dc.path().equals(previousPath) == false) {
                previousPath = dc.path();
                pathGroup++;
            }
            long h1 = pathHash(hasher, dc.path());
            EscfColumn col = batch.column(dc.leafIdx());
            // An explicit null adds nothing to the parsed document unless the field maps it to a null_value, so it is only a
            // dimension of the row in that case
            MappedFieldType fieldType = context.mappingLookup().getFieldType(dc.path());
            boolean nullIsValue = fieldType != null && fieldType.hasNullValue();
            PresentDocIterator it = col.presentDocs();
            int r;
            while ((r = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (hashes[r] == null && lastPathGroup[r] != pathGroup && (nullIsValue || col.isNull(r) == false)) {
                    sums[r] += h1;
                    found[r] = true;
                    lastPathGroup[r] = pathGroup;
                }
            }
        }
        for (int d = 0; d < docCount; d++) {
            if (hashes[d] == null && found[d]) {
                hashes[d] = encode(sums[d]);
            }
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }
}
