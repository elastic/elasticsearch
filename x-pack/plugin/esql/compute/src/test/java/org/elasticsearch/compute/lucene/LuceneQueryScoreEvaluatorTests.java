/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.DoubleVector;

import java.io.IOException;

import static org.elasticsearch.compute.lucene.LuceneQueryScoreEvaluator.NO_MATCH_SCORE;

public class LuceneQueryScoreEvaluatorTests extends LuceneQueryEvaluatorTests<DoubleVector, DoubleVector.Builder> {
    private static final String FIELD = "g";
    public static final Scorable CONSTANT_SCORER = new Scorable() {
        @Override
        public float score() throws IOException {
            return TEST_SCORE;
        }
    };
    public static final float TEST_SCORE = 1.5f;

    @Override
    protected LuceneQueryEvaluator.DenseCollector<DoubleVector.Builder> createDensecollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newDoubleVectorFixedBuilder(max - min + 1),
            b -> b.appendDouble(NO_MATCH_SCORE),
            (b, s) -> b.appendDouble(s.score())
        );
    }

    @Override
    protected Scorable getScorer() {
        return new Scorable() {
            @Override
            public float score() throws IOException {
                return TEST_SCORE;
            }
        };
    }

    @Override
    protected Object getValueAt(DoubleVector vector, int i) {
        return vector.getDouble(i);
    }

    @Override
    protected Object valueForMatch() {
        return (double) TEST_SCORE;
    }

    @Override
    protected Object valueForNoMatch() {
        return NO_MATCH_SCORE;
    }
}
