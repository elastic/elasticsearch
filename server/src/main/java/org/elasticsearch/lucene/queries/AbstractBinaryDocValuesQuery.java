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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ZipDocIdSetIterator;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

abstract class AbstractBinaryDocValuesQuery extends Query {

    final String fieldName;
    final Predicate<BytesRef> matcher;

    AbstractBinaryDocValuesQuery(String fieldName, Predicate<BytesRef> matcher) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.matcher = Objects.requireNonNull(matcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + ".counts");

                if (values == null) {
                    return null;
                }

                var valuesWithCounts = new ZipDocIdSetIterator(values, counts);
                final TwoPhaseIterator iterator = new TwoPhaseIterator(valuesWithCounts) {
                    MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

                    @Override
                    public boolean matches() throws IOException {
                        BytesRef binaryValue = values.binaryValue();
                        int count = Math.toIntExact(counts.longValue());
                        return reader.match(binaryValue, count, matcher);
                    }

                    @Override
                    public float matchCost() {
                        return 10; // because one comparison
                    }
                };

                return new DefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    protected abstract float matchCost();

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }
}
