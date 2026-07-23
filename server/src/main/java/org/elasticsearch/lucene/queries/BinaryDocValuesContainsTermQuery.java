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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query that matches documents whose binary doc values contain a specific term. Only works with binary doc values
 * encoded using {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount}.
 */
public final class BinaryDocValuesContainsTermQuery extends Query {
    final String fieldName;
    final BytesRef containsTerm;
    // See AbstractBinaryDocValuesQuery#arrayOrderInlineNull: selects the inline-null decoder for the multi-valued fallback path.
    final boolean arrayOrderInlineNull;

    BinaryDocValuesContainsTermQuery(String fieldName, BytesRef containsTerm, boolean arrayOrderInlineNull) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.containsTerm = Objects.requireNonNull(containsTerm);
        this.arrayOrderInlineNull = arrayOrderInlineNull;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        // Captured for the binary doc values decode checkpoint below. This query is reached via rewrite() so it gets its own weight and
        // must establish the breaker itself.
        final CircuitBreaker breaker = ContextIndexSearcher.circuitBreakerOrNull(searcher);
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                // Checkpoint now that a binary doc values reader has been opened for this surviving clause/segment pair.
                ContextIndexSearcher.checkBinaryDvDecodeBreaker(breaker);

                // The optimized path returns a TwoPhaseIterator-backed iterator (see the contract
                // on tryContainsIterator). ConstantScoreScorer unwraps the TwoPhase so Lucene's
                // BulkScorer drives approximation.advance(min) + matches() within [min, max), giving
                // linear scaling under sub-segment slicing (DataPartitioning.DOC).
                String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
                DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);

                // tryContainsIterator scans the whole doc blob (including the multi-valued length-prefix framing), so it is only correct
                // for single-valued fields where no length prefixes exist.
                final DocIdSetIterator containsIter = (countsSkipper == null || countsSkipper.maxValue() == 1)
                    && values instanceof BlockLoader.OptionalColumnAtATimeReader direct ? direct.tryContainsIterator(containsTerm) : null;

                final DocIdSetIterator iterator;
                if (containsIter != null) {
                    iterator = containsIter;
                } else {
                    Predicate<BytesRef> predicate = bytes -> contains(bytes, containsTerm);
                    final NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
                    iterator = arrayOrderInlineNull
                        ? AbstractBinaryDocValuesQuery.arrayOrderInlineNullIterator(values, counts, predicate, matchCost())
                        : AbstractBinaryDocValuesQuery.multiValuedIterator(values, counts, predicate, matchCost());
                }
                return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, context.reader().maxDoc());
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    float matchCost() {
        return 10;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    public String toString(String field) {
        return "BinaryDocValuesContainsTermQuery(fieldName=" + field + ",containsTerm=" + containsTerm + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesContainsTermQuery that = (BinaryDocValuesContainsTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(containsTerm, that.containsTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, containsTerm);
    }

    public static boolean contains(BytesRef value, BytesRef term) {
        return contains(value.bytes, value.offset, value.length, term);
    }

    public static boolean contains(byte[] value, int offset, int length, BytesRef term) {
        return ESVectorUtil.contains(value, offset, length, term.bytes, term.offset, term.length);
    }
}
