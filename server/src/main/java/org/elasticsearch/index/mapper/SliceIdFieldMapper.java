/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A mapper for the {@code _id} field that encodes slice+id as a composite term
 * so that uniqueness within a shard is scoped by {@code (slice, id)}.
 */
public class SliceIdFieldMapper extends IdFieldMapper {

    public static final SliceIdFieldMapper INSTANCE = new SliceIdFieldMapper();

    private SliceIdFieldMapper() {
        super(new SliceIdFieldType());
    }

    static final class SliceIdFieldType extends AbstractIdFieldType {

        SliceIdFieldType() {}

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("Fielddata is not supported on [_id] field in slice-enabled indices.");
        }

        /** Build composite terms for an ids/terms query. Requires a concrete slice from the search context. */
        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            String sliceRouting = context.getSliceRouting();
            if (sliceRouting == null) {
                throw new IllegalArgumentException("[_id] / [ids] queries require a concrete [_slice]; [_slice=_all] is not supported");
            }
            String[] slices = sliceRouting.split(",");
            List<BytesRef> terms = new ArrayList<>(values.size() * slices.length);
            for (Object v : values) {
                String idStr = (v instanceof BytesRef br) ? br.utf8ToString() : v.toString();
                for (String slice : slices) {
                    terms.add(Uid.encodeId(Uid.compositeId(slice.trim(), idStr)));
                }
            }
            return new TermInSetQuery(name(), terms);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return termsQuery(List.of(value), context);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public String decodeStoredId(BytesRef bytes) {
            // The stored _id is encodeId("slice#id"); recover the plain, user-visible id by stripping the slice prefix.
            return Uid.idFromCompositeId(Uid.decodeId(bytes));
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return IdLoader.create(blContext.indexSettings(), blContext.mappingLookup()).blockLoader(blContext.ordinalsByteSize());
        }
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        String slice = context.sourceToParse().routing();
        assert slice != null : "_slice (routing) must be set for slice-enabled indices";
        // Store the composite (slice, id) as a standard-encoded id string so engine uniqueness is scoped by (slice, id).
        // context.id() stays the plain user id, so write responses surface the plain id.
        BytesRef uid = Uid.encodeId(Uid.compositeId(slice, context.id()));
        context.doc().add(IdFieldMapper.standardIdField(uid, Field.Store.YES));
    }

    @Override
    public String documentDescription(DocumentParserContext context) {
        return "document with id '" + context.sourceToParse().id() + "' and slice '" + context.sourceToParse().routing() + "'";
    }

    @Override
    public String documentDescription(ParsedDocument parsedDocument) {
        return "[" + parsedDocument.id() + "]";
    }
}
