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
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.Objects;

/**
 * Optimized term-equality query for binary doc values encoded with
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount}.
 *
 * <p>This query is only ever created by {@link SlowCustomBinaryDocValuesTermQuery#rewrite}, which
 * first verifies that every leaf satisfies two conditions:
 * <ol>
 *   <li>The field is single-valued ({@code countsSkipper == null || countsSkipper.maxValue() == 1}).
 *   <li>The underlying data implements
 *       {@link BlockLoader.OptionalColumnAtATimeReader#tryTermEqualIterator} and returns a
 *       non-{@code null} iterator for this term.
 * </ol>
 * Because those conditions are guaranteed at rewrite time, this class contains only the optimized
 * path — there is no fallback.
 *
 * <p>The iterator returned by {@code tryTermEqualIterator} is
 * {@link org.apache.lucene.search.TwoPhaseIterator}-backed, so Lucene's BulkScorer drives
 * {@code approximation.advance(min) + matches()} within {@code [min, max)}, giving linear scaling
 * under sub-segment slicing ({@code DataPartitioning.DOC}).
 */
final class BinaryDocValuesTermEqualQuery extends Query {

    final String fieldName;
    final BytesRef term;

    BinaryDocValuesTermEqualQuery(String fieldName, BytesRef term) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }
                // rewrite() already verified that all leaves with this field implement
                // OptionalColumnAtATimeReader and return a non-null iterator.
                final DocIdSetIterator iter = ((BlockLoader.OptionalColumnAtATimeReader) values).tryTermEqualIterator(term);
                assert iter != null : "tryTermEqualIterator returned null after rewrite() verified it would not";
                return ConstantScoreScorerSupplier.fromIterator(iter, score(), scoreMode, context.reader().maxDoc());
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    public String toString(String field) {
        return "BinaryDocValuesTermEqualQuery(fieldName=" + field + ",term=" + term + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesTermEqualQuery that = (BinaryDocValuesTermEqualQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
