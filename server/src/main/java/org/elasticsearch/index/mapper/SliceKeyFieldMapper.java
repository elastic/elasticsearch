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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.sourcebatch.MappedColumns;

import java.util.Collections;

/**
 * Writes the slice sort key of a slice-enabled index.
 * <p>
 * {@link SliceIndexing#SLICE_KEY_FIELD_NAME} holds {@code BE32(h(slice)) ++ utf8(slice)} as sorted doc values and is the
 * primary index sort. Prefixing the hash lays segments out by hash prefix, which gives merge policies coordination-free
 * partition boundaries and balanced buckets. Keeping the slice bytes behind the hash keeps slices with colliding hashes
 * as distinct, adjacent terms, and keeps term order equal to document order, which the sliced vector formats rely on.
 * <p>
 * {@link SliceIndexing#SLICE_HASH_FIELD_NAME} holds the same hash as numeric doc values with a skip index. It is not a
 * sort field: its skip-index metadata exposes a segment's hash range without reading the {@code _slice_key} terms, so a
 * search can skip segments that cannot contain a slice and a merge policy can partition on hash prefixes cheaply.
 */
public class SliceKeyFieldMapper extends MetadataFieldMapper {

    public static final String NAME = SliceIndexing.SLICE_KEY_FIELD_NAME;

    /** Field type of {@code _slice_key}; also resolved directly by {@code IndexSortConfig} before mappings are loaded. */
    public static final MappedFieldType FIELD_TYPE = new SliceKeyFieldType();

    // Must follow FIELD_TYPE: the constructor passes it to MetadataFieldMapper, which dereferences it.
    public static final SliceKeyFieldMapper INSTANCE = new SliceKeyFieldMapper();

    /** Present only on slice-enabled indices; a {@code null} mapper means the field is not mapped at all. */
    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().isSliceEnabled() ? INSTANCE : null);

    static final class SliceKeyFieldType extends MappedFieldType {

        private SliceKeyFieldType() {
            super(NAME, IndexType.skippers(), false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + NAME + "] is not searchable");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Hidden from field resolution by QueryRewriteContext; the encoded term is meaningless to users.
            throw new IllegalArgumentException("[" + NAME + "] is not fetchable");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new SortedOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
        }
    }

    private SliceKeyFieldMapper() {
        super(FIELD_TYPE);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        final String slice = context.routing();
        if (slice == null) {
            // Coordinating-node validation rejects this on slice-enabled indices; tombstones legitimately have no slice.
            return;
        }
        final BytesRef key = SliceIndexing.encodeSliceKey(slice);
        final long hash = SliceIndexing.sliceHashFromKey(key);
        addFields(context.doc(), key, hash);
        // Sliced vector search runs over child documents too (diversifying-children queries), so children carry the key
        // and hash as well. Done here, after parsing, so the nested-to-root field copy never sees these fields.
        for (LuceneDocument child : context.nonRootDocuments()) {
            addFields(child, key, hash);
        }
    }

    private static void addFields(LuceneDocument doc, BytesRef key, long hash) {
        doc.add(SortedDocValuesField.indexedField(NAME, key));
        doc.add(SortedNumericDocValuesField.indexedField(SliceIndexing.SLICE_HASH_FIELD_NAME, hash));
    }

    // Mirror the field types written by addFields so the columnar and row paths produce identical schemas.
    private static final IndexableFieldType SLICE_KEY_DV_TYPE = SortedDocValuesField.indexedField("", new BytesRef()).fieldType();
    private static final IndexableFieldType SLICE_HASH_DV_TYPE = SortedNumericDocValuesField.indexedField("", 0L).fieldType();

    @Override
    protected boolean doSupportsColumnarParse(IndexSettings indexSettings) {
        return true;
    }

    @Override
    public void preColumnarParse(BatchMappingContext context) {
        final BytesRef[] routings = context.routings();
        if (routings == null) {
            return;
        }
        final int docCount = routings.length;
        assert docCount == context.docCount() : "routings length [" + docCount + "] != docCount [" + context.docCount() + "]";
        final BytesRef[] keys = new BytesRef[docCount];
        final byte[] hashes = new byte[docCount * Long.BYTES];
        for (int d = 0; d < docCount; d++) {
            final BytesRef routing = routings[d];
            if (routing == null) {
                throw new IllegalArgumentException("unable to create [" + NAME + "] as slice is enabled but slice is null");
            }
            keys[d] = SliceIndexing.encodeSliceKey(routing);
            ByteUtils.writeLongLE(SliceIndexing.sliceHashFromKey(keys[d]), hashes, d * Long.BYTES);
        }
        context.addColumn(MappedColumns.binaryColumn(keys, NAME, SLICE_KEY_DV_TYPE));
        context.addColumn(
            MappedColumns.longColumn(
                new BytesRef(hashes),
                SliceIndexing.SLICE_HASH_FIELD_NAME,
                SLICE_HASH_DV_TYPE,
                LongColumn.NumericKind.LONG
            )
        );
    }

    @Override
    protected String contentType() {
        return NAME;
    }
}
