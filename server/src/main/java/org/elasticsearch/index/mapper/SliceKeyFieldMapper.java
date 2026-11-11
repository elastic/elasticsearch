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
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
 * The hash has no field of its own: as the key's prefix it is readable per document, per term, or per segment
 * (first/last ordinal) from the key itself. {@link SliceIndexing#SLICE_HASH_FIELD_NAME} is reserved for a future
 * hash-derived structure.
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
            // Coordinating-node validation and SliceIdFieldMapper#preParse both reject unrouted documents before this
            // runs, so reaching here is a bug. Throw rather than skip, matching preColumnarParse: a document without a
            // key sorts last and is invisible to every slice query. Tombstones are built directly by ParsedDocument and
            // never pass through the parser, so none can arrive here.
            throw new IllegalArgumentException("unable to create [" + NAME + "] as slice is enabled but slice is null");
        }
        final BytesRef key = SliceIndexing.encodeSliceKey(slice);
        context.doc().add(SortedDocValuesField.indexedField(NAME, key));
        // Sliced vector search runs over child documents too (diversifying-children queries), so children carry the key
        // as well. Done here, after parsing, so the nested-to-root field copy never sees this field.
        for (LuceneDocument child : context.nonRootDocuments()) {
            child.add(SortedDocValuesField.indexedField(NAME, key));
        }
    }

    // Mirror the field type written by postParse so the columnar and row paths produce identical schemas.
    private static final IndexableFieldType SLICE_KEY_DV_TYPE = SortedDocValuesField.indexedField("", new BytesRef()).fieldType();

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
        for (int d = 0; d < docCount; d++) {
            final BytesRef routing = routings[d];
            if (routing == null) {
                throw new IllegalArgumentException("unable to create [" + NAME + "] as slice is enabled but slice is null");
            }
            keys[d] = SliceIndexing.encodeSliceKey(routing);
        }
        context.addColumn(MappedColumns.binaryColumn(keys, NAME, SLICE_KEY_DV_TYPE));
    }

    @Override
    protected String contentType() {
        return NAME;
    }
}
