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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
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
 * A mapper for the {@code _id} field of a slice-enabled index. Each document indexes two terms into {@code _id} —
 * a slice-free <em>search</em> term {@code encodeId(id) ++ [0x00]} (drives {@code ids}/{@code term} search) and a
 * <em>compound</em> term {@code encodeId(id) ++ slice ++ [len]} (the engine identity term: uniqueness/versioning/
 * GET/delete). The stored {@code _id} value stays the plain {@code encodeId(id)}, so the user-visible id is plain.
 * See {@link Uid#encodeCompoundId(String, String)} / {@link Uid#searchTerm(String)} for the layout and why the two
 * term-spaces are structurally disjoint.
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

        /**
         * Seek the slice-free search term {@code encodeId(x) ++ [0x00]} for each value. This is derived only from the
         * id, so {@code ids}/{@code term} search needs no slice context and works across slices (incl. {@code _slice=_all}).
         */
        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            List<BytesRef> terms = new ArrayList<>(values.size());
            for (Object v : values) {
                String idStr = (v instanceof BytesRef br) ? br.utf8ToString() : v.toString();
                terms.add(Uid.searchTerm(idStr));
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
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        String slice = context.sourceToParse().routing();
        assert slice != null : "_slice (routing) must be set for slice-enabled indices";
        final String id = context.id();
        // Stored _id stays plain so GET/_source/synthetic-source surface the user id unchanged.
        context.doc().add(new StoredField(NAME, Uid.encodeId(id)));
        // Slice-free search term drives ids/term search; the compound term (== Engine.Operation.uid()) scopes
        // uniqueness/versioning/GET/delete by (slice, id). Both are indexed-only (not stored).
        context.doc().add(new StringField(NAME, Uid.searchTerm(id), Field.Store.NO));
        context.doc().add(new StringField(NAME, Uid.encodeCompoundId(id, slice), Field.Store.NO));
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
