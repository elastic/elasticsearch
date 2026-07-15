/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equivalent of {@link org.apache.lucene.document.SortedDocValuesField#newSlowExactQuery(String, BytesRef)},
 * but then for binary doc values.
 * <p>
 * When the underlying codec implements
 * {@link BlockLoader.OptionalColumnAtATimeReader#tryTermEqualIterator} and the field is
 * single-valued, an optimized two-phase iterator is used per leaf; otherwise the query falls back
 * to the slower per-document predicate scan inherited from {@link AbstractBinaryDocValuesQuery}.
 * The decision is made independently per leaf, so mixed indices benefit from the fast path where
 * available.
 */
public class ScanningBinaryDocValuesTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef term;

    public ScanningBinaryDocValuesTermQuery(String fieldName, BytesRef term, boolean arrayOrderInlineNull) {
        super(fieldName, term::equals, arrayOrderInlineNull);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (term.length == 0) {
            return new BinaryDocValuesLengthQuery(fieldName, 0, arrayOrderInlineNull);
        }
        return this;
    }

    @Override
    protected DocIdSetIterator getDocIdSetIterator(LeafReaderContext context, float matchCost) throws IOException {
        final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
        DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
        // tryTermEqualIterator is only valid for single-valued fields (see its javadoc on
        // BlockLoader.OptionalColumnAtATimeReader). It returns a TwoPhaseIterator-backed iterator,
        // so sub-segment slicing (DataPartitioning.DOC) scales with cores.
        if ((countsSkipper == null || countsSkipper.maxValue() <= 1) && values instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
            DocIdSetIterator iter = direct.tryTermEqualIterator(term);
            if (iter != null) {
                return iter;
            }
        }
        // Fall back to the scanning two-phase path (handles multi-valued via the counts field).
        return super.getDocIdSetIterator(context, matchCost);
    }

    @Override
    protected float matchCost() {
        return 10; // because one comparison
    }

    @Override
    public String toString(String field) {
        return "ScanningBinaryDocValuesTermQuery(fieldName=" + fieldName + ",term=" + term.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesTermQuery that = (ScanningBinaryDocValuesTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
