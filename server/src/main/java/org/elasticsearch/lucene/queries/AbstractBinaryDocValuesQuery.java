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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueArrayOrderInlineNullBinaryDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

abstract class AbstractBinaryDocValuesQuery extends Query {

    protected final String fieldName;
    protected final Predicate<BytesRef> matcher;
    // Whether the field stores its multi-valued binary doc values in the ArrayOrderInlineNull format rather than the SeparateCount format.
    // The two encodings are not interchangeable, so the decoder must be chosen up front.
    final boolean arrayOrderInlineNull;

    AbstractBinaryDocValuesQuery(String fieldName, Predicate<BytesRef> matcher, boolean arrayOrderInlineNull) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.matcher = Objects.requireNonNull(matcher);
        this.arrayOrderInlineNull = arrayOrderInlineNull;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final DocIdSetIterator iterator = getDocIdSetIterator(context, matchCost);
                if (iterator == null) {
                    return null;
                }
                return new DefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    protected DocIdSetIterator getDocIdSetIterator(LeafReaderContext context, float matchCost) throws IOException {
        final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
        if (arrayOrderInlineNull) {
            // ArrayOrderInlineNull always writes the .counts field (even for an all-null or empty array, which writes no blob), so when
            // this flag is set the counts column drives iteration and count==1 is handled inside the inline-null reader as the raw case.
            assert counts != null : "ArrayOrderInlineNull field [" + fieldName + "] must have a " + COUNT_FIELD_SUFFIX + " companion";
            return arrayOrderInlineNullIterator(values, counts, matcher, matchCost);
        }
        if (counts != null) {
            return multiValuedIterator(values, counts, matcher, matchCost);
        } else {
            return singleValuedIterator(values, matcher, matchCost);
        }
    }

    protected abstract float matchCost();

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    static DocIdSetIterator multiValuedIterator(
        BinaryDocValues values,
        NumericDocValues counts,
        Predicate<BytesRef> predicate,
        float cost
    ) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
            final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

            @Override
            public boolean matches() throws IOException {
                values.advance(counts.docID());
                return reader.match(values.binaryValue(), counts.longValue(), predicate);
            }

            @Override
            public float matchCost() {
                return cost;
            }
        });
    }

    /**
     * Iterator for the {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull}
     * format. Drives the two-phase approximation on the always-present {@code counts} column, then advances the binary blob with
     * {@link BinaryDocValues#advanceExact} for the candidate doc: a document with no blob is an all-null or empty array (the counts field
     * is present but no binary value was written) and can never match a term-family predicate, so it is rejected without decoding.
     */
    static DocIdSetIterator arrayOrderInlineNullIterator(
        BinaryDocValues values,
        NumericDocValues counts,
        Predicate<BytesRef> predicate,
        float cost
    ) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
            final MultiValueArrayOrderInlineNullBinaryDocValuesReader reader = new MultiValueArrayOrderInlineNullBinaryDocValuesReader();

            @Override
            public boolean matches() throws IOException {
                if (values.advanceExact(counts.docID()) == false) {
                    return false; // all-null array or empty array: no non-null value to match
                }
                return reader.match(values.binaryValue(), counts.longValue(), predicate);
            }

            @Override
            public float matchCost() {
                return cost;
            }
        });
    }

    /**
     * Iterator for the {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull
     * KeyedArrayOrderInlineNull} format used by flattened fields in columnar mode. Unlike
     * {@link #arrayOrderInlineNullIterator}, slots carry a {@code key\0value} prefix so the predicate tests the full
     * keyed value and the reader must skip past the key to find it.
     */
    protected static DocIdSetIterator keyedInlineNullIterator(
        BinaryDocValues values,
        NumericDocValues counts,
        Predicate<BytesRef> predicate,
        float cost
    ) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
            final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

            @Override
            public boolean matches() throws IOException {
                values.advance(counts.docID());
                return reader.matchKeyedInlineNull(values.binaryValue(), counts.longValue(), predicate);
            }

            @Override
            public float matchCost() {
                return cost;
            }
        });
    }

    static DocIdSetIterator singleValuedIterator(BinaryDocValues values, Predicate<BytesRef> predicate, float cost) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(values) {
            final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

            @Override
            public boolean matches() throws IOException {
                return reader.match(values.binaryValue(), 1, predicate);
            }

            @Override
            public float matchCost() {
                return cost;
            }
        });
    }
}
