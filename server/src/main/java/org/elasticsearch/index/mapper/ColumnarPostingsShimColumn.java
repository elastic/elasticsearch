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
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.TokenStreamColumn;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * Carries a field's index options for the documents of a batch that hold no value, so their schema matches the field's.
 *
 * <p>A document holding only nulls has the ColumNAR codec's payload and no term. Lucene compares a document's schema for a field against
 * the field's own and rejects a document that indexes it differently, so such a document indexes the field too — with a token stream that
 * yields nothing, which adds no term. {@code SyntheticIdField} indexes the synthetic {@code _id} the same way and for the same reason.
 *
 * <p>The payload itself stays on the doc-values column beside this one: two instances of a field in one document are what the row path
 * writes as one, and Lucene reads the document's schema from all of them together.
 */
public final class ColumnarPostingsShimColumn extends TokenStreamColumn implements LuceneColumn {

    /** The documents of the batch that hold no value, by their place in it. */
    private final FixedBitSet valueless;
    private final int from;
    private final int count;
    private final FixedBitSet filter;

    public ColumnarPostingsShimColumn(String name, IndexableFieldType shimType, FixedBitSet valueless, int count) {
        this(name, shimType, valueless, 0, count, null);
    }

    private ColumnarPostingsShimColumn(
        String name,
        IndexableFieldType shimType,
        FixedBitSet valueless,
        int from,
        int count,
        FixedBitSet filter
    ) {
        super(name, shimType, Density.SPARSE);
        this.valueless = valueless;
        this.from = from;
        this.count = count;
        this.filter = filter;
    }

    @Override
    public LuceneColumn withFilter(FixedBitSet newFilter) {
        assert newFilter == null || newFilter.length() == count;
        return new ColumnarPostingsShimColumn(name(), fieldType(), valueless, from, count, LuceneColumn.singleFilter(filter, newFilter));
    }

    @Override
    public LuceneColumn slice(int from, int count) {
        Objects.checkFromIndexSize(from, count, this.count);
        return new ColumnarPostingsShimColumn(name(), fieldType(), valueless, this.from + from, count, windowValidity(filter, from, count));
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        return new LuceneColumn.RowFieldCursor() {
            private final ObjectTupleCursor<TokenStream> tuples = tuples();

            @Override
            public int nextDoc() {
                return tuples.nextDoc();
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                out.add(new Field(name(), tuples.value(), fieldType()));
            }
        };
    }

    @Override
    public ObjectTupleCursor<TokenStream> tuples() {
        return new ObjectTupleCursor<>() {
            private final ColumnarBinaryDocValuesField.EmptyTokenStream tokens = new ColumnarBinaryDocValuesField.EmptyTokenStream();
            private int doc = -1;

            @Override
            public int nextDoc() {
                while (++doc < count) {
                    if (valueless.get(from + doc) && (filter == null || filter.get(doc))) {
                        return doc;
                    }
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public TokenStream value() {
                return tokens;
            }
        };
    }
}
