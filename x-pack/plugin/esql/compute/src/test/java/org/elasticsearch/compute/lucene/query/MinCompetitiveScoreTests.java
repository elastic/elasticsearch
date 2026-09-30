/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for {@link MinCompetitiveScore}, fed by a real {@link TopNOperator} the way production is.
 */
public class MinCompetitiveScoreTests extends ComputeTestCase {
    static final SharedMinCompetitive.KeyConfig SCORE_DESC = new SharedMinCompetitive.KeyConfig(
        ElementType.DOUBLE,
        TopNEncoder.DEFAULT_SORTABLE,
        false,
        true
    );

    public void testRejectsAscending() {
        SharedMinCompetitive.KeyConfig asc = new SharedMinCompetitive.KeyConfig(
            ElementType.DOUBLE,
            TopNEncoder.DEFAULT_SORTABLE,
            true,
            false
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new MinCompetitiveScore.Factory(new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(asc)))
        );
    }

    public void testRejectsNonDouble() {
        SharedMinCompetitive.KeyConfig l = new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, false, true);
        expectThrows(
            IllegalArgumentException.class,
            () -> new MinCompetitiveScore.Factory(new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(l)))
        );
    }

    public void testRejectsMultipleKeys() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new MinCompetitiveScore.Factory(
                new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(SCORE_DESC, SCORE_DESC))
            )
        );
    }

    /**
     * The published bound must be exactly the K-th best score seen so far once the heap is full,
     * nothing before that, and it must never go down.
     */
    public void testTracksTopNHeapTop() {
        BlockFactory blockFactory = blockFactory();
        int topCount = between(1, 20);
        SharedMinCompetitive.Supplier supplier = new SharedMinCompetitive.Supplier(blockFactory.breaker(), List.of(SCORE_DESC));
        MinCompetitiveScore.Factory factory = new MinCompetitiveScore.Factory(supplier);
        try (
            TopNOperator topN = new TopNOperator(
                blockFactory,
                blockFactory.breaker(),
                topCount,
                List.of(ElementType.DOUBLE),
                List.of(TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, false, true)),
                between(1, 1000),
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                supplier
            );
            MinCompetitiveScore minCompetitiveScore = factory.build(blockFactory)
        ) {
            List<Float> seen = new ArrayList<>();
            float previous = MinCompetitiveScore.NO_THRESHOLD;
            int pages = between(1, 30);
            for (int p = 0; p < pages; p++) {
                int positions = between(1, 10);
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positions)) {
                    for (int i = 0; i < positions; i++) {
                        // Lucene scores are floats and ES|QL widens them to double.
                        float score = randomBoolean() ? randomFloatBetween(0, 100, true) : between(0, 5);
                        seen.add(score);
                        builder.appendDouble(score);
                    }
                    topN.addInput(new Page(builder.build()));
                }
                float current = minCompetitiveScore.minCompetitiveScore();
                assertThat(current, greaterThanOrEqualTo(previous));
                previous = current;
                if (seen.size() < topCount) {
                    assertThat(current, equalTo(MinCompetitiveScore.NO_THRESHOLD));
                } else {
                    List<Float> sorted = new ArrayList<>(seen);
                    sorted.sort(Collections.reverseOrder());
                    float kth = sorted.get(topCount - 1);
                    // 0 is reported as NO_THRESHOLD, which is the same value.
                    assertThat(current, equalTo(kth));
                }
            }
            // Reading again without new input doesn't decode again.
            int decodes = minCompetitiveScore.decodes();
            assertThat(minCompetitiveScore.minCompetitiveScore(), equalTo(previous));
            assertThat(minCompetitiveScore.decodes(), equalTo(decodes));
        }
    }

    public void testToMinCompetitiveFloatExactForFloats() {
        float f = randomFloatBetween(Float.MIN_VALUE, Float.MAX_VALUE, true);
        assertThat(MinCompetitiveScore.toMinCompetitiveFloat(f), equalTo(f));
    }

    public void testToMinCompetitiveFloatRoundsDown() {
        double d = randomDoubleBetween(Float.MIN_VALUE, Float.MAX_VALUE, true);
        float f = MinCompetitiveScore.toMinCompetitiveFloat(d);
        assertThat((double) f, lessThanOrEqualTo(d));
        // And it is the largest float that is <= d
        assertThat((double) Math.nextUp(f), greaterThanOrEqualTo(d));
    }

    public void testToMinCompetitiveFloatNoThreshold() {
        assertThat(MinCompetitiveScore.toMinCompetitiveFloat(Double.NaN), equalTo(MinCompetitiveScore.NO_THRESHOLD));
        assertThat(MinCompetitiveScore.toMinCompetitiveFloat(-randomDouble()), equalTo(MinCompetitiveScore.NO_THRESHOLD));
        assertThat(MinCompetitiveScore.toMinCompetitiveFloat(0), equalTo(MinCompetitiveScore.NO_THRESHOLD));
    }
}
