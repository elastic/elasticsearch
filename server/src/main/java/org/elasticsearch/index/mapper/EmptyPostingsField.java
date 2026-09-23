/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableFieldType;

/**
 * An indexed field that contributes no term: it registers a field's index options for a document that has the field but nothing to
 * index under it.
 *
 * <p>Lucene builds a field's {@code FieldInfo} from the first document that carries it and rejects any later document that presents
 * the same field with different index options. A document normally either holds a value, giving the field its index options, or
 * leaves the field out altogether. A field whose doc values are stored by the ColumNAR codec has a third shape: its payload carries
 * its own slot count, so it joins the document as soon as the field appears — including for an array of nothing but nulls, where
 * there is no value to index. Without this field, such a document would carry the field as doc values alone and be rejected.
 *
 * <p>The field writes no term, so nothing becomes findable through it; an indexed field with norms gets its norm as it otherwise
 * would.
 */
public final class EmptyPostingsField extends Field {

    /** Produces no tokens, so the field is inverted into nothing. */
    private static final class EmptyTokenStream extends TokenStream {
        @Override
        public boolean incrementToken() {
            return false;
        }
    }

    /**
     * @param type built by {@link #typeFor}, and only for a field whose values are indexed
     */
    public EmptyPostingsField(String fieldName, IndexableFieldType type) {
        super(fieldName, new EmptyTokenStream(), type);
    }

    /**
     * The type this field takes for values indexed as {@code indexed}: the index options, norms and term vectors that field carries —
     * the three Lucene keeps in a {@code FieldInfo} and compares across documents — but tokenized and unstored, which is what a
     * {@link TokenStream} field requires and neither of which reaches {@code FieldInfo}.
     *
     * <p>Meant to be built once per mapper and reused, so documents taking this shape hand Lucene the same frozen type each time.
     */
    public static FieldType typeFor(FieldType indexed) {
        final FieldType type = new FieldType();
        type.setIndexOptions(indexed.indexOptions());
        type.setOmitNorms(indexed.omitNorms());
        type.setStoreTermVectors(indexed.storeTermVectors());
        if (indexed.storeTermVectors()) {
            type.setStoreTermVectorPositions(indexed.storeTermVectorPositions());
            type.setStoreTermVectorOffsets(indexed.storeTermVectorOffsets());
            type.setStoreTermVectorPayloads(indexed.storeTermVectorPayloads());
        }
        type.setTokenized(true);
        type.setStored(false);
        type.freeze();
        return type;
    }

}
