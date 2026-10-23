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
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnKind;
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
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

/**
 * Mapper for the {@code _dimension_group} field of time series indices that sort on it (see
 * {@link IndexSettings#TIME_SERIES_DIMENSION_GROUP_FIELD}).
 * <p>
 * The field holds a copy of the value of the configured dimension field, by default {@code resource.attributes.host.name}, so that
 * the index sort can place the time series of one metric that come from the same host next to each other. Only keyword-like values
 * are copied; a document without the field, or with a numeric value, has no {@code _dimension_group} and sorts last within its
 * metric. The copy is made on the data node from the parsed document, so no extra data travels from the coordinating node.
 */
public class DimensionGroupFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_dimension_group";

    public static final DimensionGroupFieldType FIELD_TYPE = new DimensionGroupFieldType();

    public static final DimensionGroupFieldMapper INSTANCE = new DimensionGroupFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> c.getIndexSettings().useTimeSeriesDimensionGroupSort() ? INSTANCE : null
    );

    public static final class DimensionGroupFieldType extends MappedFieldType {

        private DimensionGroupFieldType() {
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
            return DocValueFormat.RAW;
        }
    }

    private DimensionGroupFieldMapper() {
        super(FIELD_TYPE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        String groupField = context.indexSettings().getTimeSeriesDimensionGroupField();
        if (isDimension(context, groupField) == false) {
            return;
        }
        IndexableField field = context.rootDoc().getField(groupField);
        if (field == null) {
            return;
        }
        BytesRef value = field.binaryValue();
        if (value != null) {
            context.rootDoc().add(new SortedDocValuesField(NAME, value));
        }
    }

    /** Mirrors the {@code new SortedDocValuesField(NAME, ...)} written in {@link #postParse}. */
    private static final IndexableFieldType DV_TYPE = new SortedDocValuesField("", new BytesRef()).fieldType();

    @Override
    protected boolean doSupportsColumnarParse(IndexSettings indexSettings) {
        // The dimension check needs the index.dimensions predicate; routing_path indices stay on the row path, where the dimensions
        // are collected while parsing.
        return indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions;
    }

    @Override
    public void postColumnarParse(BatchMappingContext context) {
        String groupField = context.indexSettings().getTimeSeriesDimensionGroupField();
        if (isDimension(context.indexSettings(), context.mappingLookup(), groupField) == false) {
            return;
        }
        EscfBatch batch = escfBatch(context.sourceBatch());
        int leaf = leafIndexOf(batch.schema(), groupField);
        if (leaf < 0) {
            return;
        }
        BytesRef[] values = firstStringValues(batch.column(leaf), context.docCount());
        context.addColumn(MappedColumns.binaryColumn(values, NAME, DV_TYPE));
    }

    /**
     * Whether {@code field} is a dimension of the index, i.e. its value is part of the tsid. The fields that feed the sort keys
     * ahead of {@code _tsid} ({@code _dimension_group} and {@code _metric_name_hash}) must be, so that the documents of one tsid
     * all carry the same sort keys and stay contiguous in a segment, which the synthetic {@code _id} lookup relies on (see
     * {@code OrdinalRunsDocValues}). The dimensions are the fields matching {@code index.dimensions} when that is set, and
     * otherwise the fields mapped with {@code time_series_dimension}.
     */
    static boolean isDimension(IndexSettings indexSettings, MappingLookup mappingLookup, String field) {
        if (indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions) {
            return forIndexDimensions.matchesField(field);
        }
        MappedFieldType fieldType = mappingLookup.getFieldType(field);
        return fieldType != null && fieldType.isDimension();
    }

    /**
     * The row path variant of {@link #isDimension(IndexSettings, MappingLookup, String)}: on a {@code routing_path} index the
     * dimensions of the document were collected while parsing it, which also covers the dynamically mapped ones (e.g. under a
     * {@code passthrough} object) that the mapping lookup does not know yet.
     */
    static boolean isDimension(DocumentParserContext context, String field) {
        if (context.getRoutingFields() instanceof RoutingPathFields routingPathFields
            && context.indexSettings().getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
            return routingPathFields.routingValues().containsKey(new BytesRef(field));
        }
        return isDimension(context.indexSettings(), context.mappingLookup(), field);
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    // Helpers shared with MetricNameHashFieldMapper, which reads source columns the same way.

    static EscfBatch escfBatch(SourceBatch batch) {
        if (batch instanceof EscfBatch escfBatch) {
            return escfBatch;
        }
        throw new UnsupportedOperationException("[" + NAME + "] requires an EscfBatch; got " + batch.getClass().getSimpleName());
    }

    /** The index of the leaf column at {@code fullPath}, or {@code -1} if the batch has none. */
    static int leafIndexOf(SourceSchema schema, String fullPath) {
        int leafCount = schema.leafCount();
        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            if (fullPath.equals(schema.getFullPath(leafIdx))) {
                return leafIdx;
            }
        }
        return -1;
    }

    /**
     * The first string value of each row of {@code col}, as an owned copy, or {@code null} for rows without one. Mirrors what the
     * row path sees in the parsed document: a keyword field's first value, and nothing for numeric or boolean values.
     */
    static BytesRef[] firstStringValues(EscfColumn col, int docCount) {
        BytesRef[] values = new BytesRef[docCount];
        byte kind = col.kind();
        switch (kind) {
            case EscfColumnKind.STRING, EscfColumnKind.ARRAY -> {
                if (col.leafValueKind() != EscfColumnKind.STRING) {
                    return values;
                }
                // An array column yields one tuple per element with the row repeated, so keeping the first keeps the first element.
                ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
                int r;
                while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (values[r] == null) {
                        values[r] = BytesRef.deepCopyOf(cursor.value());
                    }
                }
            }
            case EscfColumnKind.UNION -> {
                ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
                int r;
                while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    byte typeByte = col.getTypeByte(r);
                    if (typeByte == SourceValueType.STRING) {
                        values[r] = BytesRef.deepCopyOf(cursor.value());
                    } else if (typeByte == SourceValueType.FIXED_ARRAY || typeByte == SourceValueType.UNION_ARRAY) {
                        values[r] = firstStringElement(col.getArrayValue(r));
                    }
                }
            }
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.BOOL, EscfColumnKind.BINARY -> {
                // not keyword-like; the row path adds no value for these either
            }
            default -> throw new IllegalArgumentException("Unexpected column kind for [" + NAME + "]: " + EscfColumnKind.name(kind));
        }
        return values;
    }

    private static BytesRef firstStringElement(ArrayReader ar) {
        while (ar.next()) {
            if (ar.isNull()) {
                continue;
            }
            if (ar.type() == SourceValueType.STRING) {
                XContentString.UTF8Bytes utf8 = ar.textValue().bytes();
                return new BytesRef(Arrays.copyOfRange(utf8.bytes(), utf8.offset(), utf8.offset() + utf8.length()));
            }
            return null;
        }
        return null;
    }
}
