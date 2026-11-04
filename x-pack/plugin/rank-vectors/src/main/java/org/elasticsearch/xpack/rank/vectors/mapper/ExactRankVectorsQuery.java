/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Scores every document that has binary {@code rank_vectors} doc values using exact MaxSim.
 * This is the multivector counterpart of the native exact dense-vector query.
 */
final class ExactRankVectorsQuery extends Query {

    private static final Runnable NO_CANCELLATION = () -> {};

    private final String field;
    private final float[][] queryVectors;
    private final int dimensions;

    ExactRankVectorsQuery(String field, float[][] queryVectors, int dimensions) {
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.dimensions = dimensions;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        Runnable checkCancelled = searcher instanceof ContextIndexSearcher contextSearcher
            ? contextSearcher::checkCancelled
            : NO_CANCELLATION;
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                ScorerSupplier supplier = scorerSupplier(context);
                if (supplier == null) {
                    return Explanation.noMatch("No rank vectors found for field [" + field + "]");
                }
                Scorer scorer = supplier.get(1L);
                if (scorer.iterator().advance(doc) != doc) {
                    return Explanation.noMatch("Document has no rank vectors for field [" + field + "]");
                }
                return Explanation.match(scorer.score(), "exact MaxSim score");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                BinaryDocValues values = DocValues.getBinary(context.reader(), field);
                if (values.cost() == 0) {
                    return null;
                }
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        return new Scorer() {
                            @Override
                            public DocIdSetIterator iterator() {
                                return values;
                            }

                            @Override
                            public float getMaxScore(int upTo) {
                                return Float.POSITIVE_INFINITY;
                            }

                            @Override
                            public float score() throws IOException {
                                checkCancelled.run();
                                float rawScore = MuveraMaxSimQuery.exactMaxSim(
                                    queryVectors,
                                    values.binaryValue(),
                                    dimensions,
                                    checkCancelled
                                );
                                return MuveraMaxSimQuery.score(rawScore) * boost;
                            }

                            @Override
                            public int docID() {
                                return values.docID();
                            }
                        };
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext context) {
                return DocValues.isCacheable(context, field);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "ExactRankVectorsQuery(field=" + this.field + ")";
    }

    @Override
    public boolean equals(Object object) {
        if (sameClassAs(object) == false) {
            return false;
        }
        ExactRankVectorsQuery other = (ExactRankVectorsQuery) object;
        return field.equals(other.field) && Arrays.deepEquals(queryVectors, other.queryVectors) && dimensions == other.dimensions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, Arrays.deepHashCode(queryVectors), dimensions);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
