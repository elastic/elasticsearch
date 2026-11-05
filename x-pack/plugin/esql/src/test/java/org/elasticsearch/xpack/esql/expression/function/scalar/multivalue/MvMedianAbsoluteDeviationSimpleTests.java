/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

public class MvMedianAbsoluteDeviationSimpleTests extends ESTestCase {
    /**
     * Sorted values are computed by {@code ascending}, which must reject a {@code NaN} like the unsorted path does, even
     * when the median itself is finite.
     */
    public void testAscendingRejectsNaN() {
        try (DoubleBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(3)) {
            builder.beginPositionEntry();
            builder.appendDouble(1.0);
            builder.appendDouble(2.0);
            builder.appendDouble(Double.NaN);
            builder.endPositionEntry();
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            try (DoubleBlock block = builder.build()) {
                ArithmeticException e = expectThrows(
                    ArithmeticException.class,
                    () -> MvMedianAbsoluteDeviation.ascending(new MvMedianAbsoluteDeviation.Doubles(), block, 0, 3)
                );
                assertEquals("not a finite double number: NaN", e.getMessage());
            }
        }
    }
}
