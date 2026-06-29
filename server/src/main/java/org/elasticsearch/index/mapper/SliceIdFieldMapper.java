/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
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
 * GET/delete). The compound bytes are also the value stored in the {@code _id} field itself: in {@link #DOCUMENT}
 * mode as a stored field, and in {@link #COLUMNAR} mode as binary doc values. This uniform storage (live docs and
 * delete tombstones alike carry the compound) keeps the engine, recovery, and translog paths free of live-vs-tombstone
 * branching. The user-visible plain id and the slice are recovered at the presentation layer only, via
 * {@link IdFieldMapper#decodeIdentity} / {@link Uid#decodeCompoundId(BytesRef)} /
 * {@link Uid#sliceFromCompoundId(BytesRef)} — mirroring TSDB's synthetic-id approach. See
 * {@link Uid#encodeCompoundId(String, String)} / {@link Uid#searchTerm(String)} for the layout and why the two
 * term-spaces are structurally disjoint.
 */
public class SliceIdFieldMapper extends IdFieldMapper {

    /** The plain id is kept in a stored field (with an inverted index of the search/compound terms). */
    public static final SliceIdFieldMapper DOCUMENT = new SliceIdFieldMapper(false);
    /** The plain id is kept in binary doc values (no stored field), for use with columnar {@code _id} mode. */
    public static final SliceIdFieldMapper COLUMNAR = new SliceIdFieldMapper(true);

    private final boolean columnar;

    private SliceIdFieldMapper(boolean columnar) {
        super(new SliceIdFieldType(columnar));
        this.columnar = columnar;
    }

    @Override
    public boolean isColumnarMode() {
        return columnar;
    }

    static final class SliceIdFieldType extends AbstractIdFieldType {

        SliceIdFieldType(boolean columnar) {
            super(columnar);
        }

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
        if (slice == null) {
            // Coordinating-node validation normally rejects this first, but parsing can be reached on paths that bypass it,
            // so fail with the same message as IdFieldMapper.encodeIdentity rather than NPE in Uid.encodeCompoundId below.
            throw new IllegalArgumentException("unable to create _id as slice is enabled but _slice is null");
        }
        final String id = context.id();
        // Slice-free search term drives ids/term search; the compound term (== Engine.Operation.uid()) scopes
        // uniqueness/versioning/GET/delete by (slice, id). Both are indexed-only (not stored), in both modes.
        context.doc().add(new StringField(NAME, Uid.searchTerm(id), Field.Store.NO));
        final BytesRef compound = Uid.encodeCompoundId(id, slice);
        context.doc().add(new StringField(NAME, compound, Field.Store.NO));
        // The compound bytes are also stored as the _id value (stored field in document mode, binary doc values in
        // columnar mode). Live docs and delete tombstones carry the same compound bytes, eliminating any live-vs-tombstone
        // branching in the engine and recovery paths. The plain user id and slice are recovered at the presentation layer
        // only (IdFieldMapper.decodeIdentity / Uid.decodeCompoundId).
        if (columnar) {
            context.doc().add(new BinaryDocValuesField(NAME, compound));
        } else {
            context.doc().add(new StoredField(NAME, compound));
        }
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (columnar) {
            // Mirrors ProvidedIdFieldMapper#postParse. Nested (non-root) documents share the root's compound _id so that
            // binary doc values on every segment doc carry the same opaque identity bytes as the root doc.
            var iterator = context.nonRootDocuments().iterator();
            if (iterator.hasNext()) {
                final BytesRef compound = Uid.encodeCompoundId(context.id(), context.sourceToParse().routing());
                while (iterator.hasNext()) {
                    iterator.next().add(new BinaryDocValuesField(NAME, compound));
                }
            }
        }
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
