/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ScoreOperator;

import java.io.IOException;

import static org.elasticsearch.compute.lucene.query.LuceneQueryScoreEvaluator.NO_MATCH_SCORE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class LuceneQueryScoreEvaluatorTests extends LuceneQueryEvaluatorTests<DoubleBlock, DoubleBlock.Builder> {

    private static final float TEST_SCORE = 1.5f;
    private static final Double DEFAULT_SCORE = 1.0;

    @Override
    protected LuceneQueryEvaluator.DenseCollector<DoubleBlock.Builder> createDenseCollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newDoubleBlockBuilder(max - min + 1),
            null,
            b -> b.appendDouble(NO_MATCH_SCORE),
            (b, s) -> b.appendDouble(s.score()),
            null
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
    protected Operator createOperator(DriverContext ctx, IndexedByShardId<LuceneQueryEvaluator.ShardConfig> shards) {
        return new ScoreOperator(ctx.blockFactory(), new LuceneQueryScoreEvaluator(ctx.blockFactory(), shards), 1);
    }

    @Override
    protected boolean usesScoring() {
        return true;
    }

    @Override
    protected int resultsBlockIndex(Page page) {
        // Reuses the score block
        return 1;
    }

    @Override
    protected void assertCollectedResultMatch(DoubleBlock resultVector, int position, boolean isMatch) {
        if (isMatch) {
            assertThat(resultVector.getDouble(position), equalTo((double) TEST_SCORE));
        } else {
            // All docs have a default score coming from Lucene
            assertThat(resultVector.getDouble(position), equalTo(NO_MATCH_SCORE));
        }
    }

    @Override
    protected void assertTermResultMatch(DoubleBlock resultVector, int position, boolean isMatch) {
        if (isMatch) {
            assertThat(resultVector.getDouble(position), greaterThan(DEFAULT_SCORE));
        } else {
            // All docs have a default score coming from Lucene
            assertThat(resultVector.getDouble(position), equalTo(DEFAULT_SCORE));
        }
    }
}
