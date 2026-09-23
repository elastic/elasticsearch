/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.TokenStreamColumn;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.EmptyPostingsField;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

/**
 * The batch-path counterpart of {@link EmptyPostingsField}: it registers a field's index options for the documents that carry the
 * field but have nothing to index under it, and writes no term for any of them.
 *
 * <p>A {@link TokenStreamColumn} is the column that carries only an inverted-index aspect, which is what is wanted here — the values
 * themselves travel in the doc-values column added under the same name. Every document in {@code docs} is given the same empty
 * {@link TokenStream}, so the column inverts into nothing.
 *
 * @see EmptyPostingsField for why a document with no value still has to give the field its index options
 */
public final class LuceneEmptyPostingsColumn extends TokenStreamColumn implements LuceneColumn {

    /** Produces no tokens. Stateless, so one instance serves every document in the batch. */
    private static final class EmptyTokenStream extends TokenStream {
        @Override
        public boolean incrementToken() {
            return false;
        }
    }

    private final FixedBitSet docs;
    private final int docCount;

    /**
     * @param docs the documents to register the field for, by batch-local doc id
     * @param docCount the number of documents in the batch
     */
    public LuceneEmptyPostingsColumn(String name, IndexableFieldType fieldType, FixedBitSet docs, int docCount) {
        super(name, fieldType, Density.SPARSE);
        this.docs = docs;
        this.docCount = docCount;
    }

    /** Whether any document needs the field registered, so callers can skip adding an empty column. */
    public boolean isEmpty() {
        return docs.cardinality() == 0;
    }

    @Override
    public ObjectTupleCursor<TokenStream> tuples() {
        return new ObjectTupleCursor<>() {
            private final TokenStream tokens = new EmptyTokenStream();
            private int doc = -1;

            @Override
            public int nextDoc() {
                doc = doc + 1 >= docs.length() ? DocIdSetIterator.NO_MORE_DOCS : docs.nextSetBit(doc + 1);
                return doc;
            }

            @Override
            public TokenStream value() {
                return tokens;
            }
        };
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    /**
     * A batch settles the field's index options on the column carrying its values, so nothing has to be registered separately there —
     * and Lucene rejects a second column claiming inversion for the same field. This column is for the row path alone, which builds a
     * document at a time and so needs the field to appear in the documents that have no value for it.
     */
    @Override
    public boolean appearsInColumnBatch() {
        return false;
    }

    @Override
    public LuceneColumn slice(int from, int count) {
        return new LuceneEmptyPostingsColumn(name(), fieldType(), EscfColumn.windowValidity(docs, from, count), count);
    }

    @Override
    public LuceneColumn withFilter(FixedBitSet filter) {
        if (filter == null) {
            return this;
        }
        FixedBitSet filtered = docs.clone();
        filtered.and(filter);
        return new LuceneEmptyPostingsColumn(name(), fieldType(), filtered, docCount);
    }

    @Override
    public RowFieldCursor rowFieldCursor() {
        return new RowFieldCursor() {
            private int doc = -1;

            @Override
            public int nextDoc() {
                doc = doc + 1 >= docs.length() ? DocIdSetIterator.NO_MORE_DOCS : docs.nextSetBit(doc + 1);
                return doc;
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                out.add(EmptyPostingsField.create(name(), fieldType()));
            }
        };
    }
}
