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
 * A mapper for the {@code _id} field of a slice-enabled index. Each document indexes two terms into {@code _id}.
 * A slice-free <em>search</em> term {@code encodeId(id + "#")} (for search) and a
 * <em>compound</em> term {@code encodeId(id + "#" + slice)} (uniqueness).
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
         * The stored {@code _id} is the compound {@code id#slice} term; strip the slice so that generic stored-field
         * readers expose the user-visible id.
         */
        @Override
        public String decodeStoredId(byte[] value) {
            return decodeCompoundId(new BytesRef(value));
        }

        /**
         * Seek the slice-free search term {@code encodeId(id + "#")} for each value. This is derived only from the
         * id, so {@code ids}/{@code term} search needs no slice context and works across slices (incl. {@code slice=_all}).
         */
        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            List<BytesRef> terms = new ArrayList<>(values.size());
            for (Object v : values) {
                String idStr = (v instanceof BytesRef br) ? br.utf8ToString() : v.toString();
                terms.add(searchTerm(idStr));
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

    /** Nested children carry the compound identity term so a soft-delete by uid removes them with their root. */
    @Override
    BytesRef nestedIdentityTerm(DocumentParserContext context) {
        return encodeCompoundId(context.id(), context.sourceToParse().routing());
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
            // so fail with the same message as IdFieldMapper.encodeIdentity rather than NPE in encodeCompoundId below.
            throw new IllegalArgumentException("unable to create _id as slice is enabled but slice is null");
        }
        final String id = context.id();
        // Slice-free search term drives ids/term search; the compound term (== Engine.Operation.uid()) scopes
        // uniqueness/versioning/GET/delete.
        context.doc().add(new StringField(NAME, searchTerm(id), Field.Store.NO));
        final BytesRef compound = encodeCompoundId(id, slice);
        context.doc().add(new StringField(NAME, compound, Field.Store.NO));
        // The compound bytes are also stored as the _id value (stored field in document mode, binary doc values in
        // columnar mode).
        if (columnar) {
            context.doc().add(new BinaryDocValuesField(NAME, compound));
        } else {
            context.doc().add(new StoredField(NAME, compound));
        }
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (columnar) {
            // Nested children are in the same Lucene updateDocuments batch as the root, which requires a consistent
            // field schema, so they carry the root's compound id in doc values too.
            var iterator = context.nonRootDocuments().iterator();
            if (iterator.hasNext()) {
                final BytesRef compound = encodeCompoundId(context.id(), context.sourceToParse().routing());
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

    /**
     * Slice-enabled {@code _id} encoding.
     * <p>
     * A slice-enabled index indexes two terms per document into the {@code _id} field:
     * <pre>
     *   search term  : encodeId(id + "#")          (drives ids/term search — empty-slice member)
     *   compound term: {@link Uid#encodeCompoundId} (uid(): uniqueness/version/GET/delete)
     * </pre>
     * {@code '#'} is not a valid slice character (see {@link org.elasticsearch.index.SliceIndexing#VALID_SLICE_VALUE_PATTERN}),
     * so the two term-spaces are structurally disjoint. The compound codec lives on {@link Uid}; these are thin aliases.
     */
    public static BytesRef encodeCompoundId(String id, String slice) {
        return Uid.encodeCompoundId(id, slice);
    }

    /**
     * The slice-mode search term {@code encodeId(id + "#")}.
     */
    public static BytesRef searchTerm(String id) {
        return Uid.encodeId(id + "#");
    }

    /** Recover the plain, user-visible id from a compound term produced above. */
    public static String decodeCompoundId(BytesRef term) {
        return Uid.fromTerm(term, true).id();
    }

    /** Strip the slice suffix from an already-decoded compound id string ({@code id#slice} to {@code id}). */
    static String stripSlice(String compound) {
        return compound.substring(0, compound.lastIndexOf('#'));
    }

    /** Recover the slice from a compound term. */
    public static String sliceFromCompoundId(BytesRef term) {
        return Uid.fromTerm(term, true).slice();
    }
}
