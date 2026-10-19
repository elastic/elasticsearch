/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

/** Evaluation-grid sizing rejects unrepresentable arrays before allocation, without overflowing timestamp arithmetic. */
public class PromqlEvaluationGridTests extends ESTestCase {
    /** The endpoint is included only when a complete step reaches it, including ranges before the Unix epoch. */
    public void testInclusiveStepCount() {
        assertEquals(1, PromqlLogicalPlanBuilder.evaluationStepCount(7, 7, 10));
        assertEquals(3, PromqlLogicalPlanBuilder.evaluationStepCount(-10, 10, 10));
        assertEquals(2, PromqlLogicalPlanBuilder.evaluationStepCount(-10, 9, 10));
    }

    /** A range wider than Long.MAX_VALUE can still contain a representable number of widely spaced steps. */
    public void testStepCountAcrossLongRange() {
        assertEquals(3, PromqlLogicalPlanBuilder.evaluationStepCount(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(1, PromqlLogicalPlanBuilder.evaluationStepCount(Long.MAX_VALUE, Long.MAX_VALUE, 1));
    }

    /** Array-size and invalid-range errors must be detected without constructing any timestamp list. */
    public void testUnrepresentableStepCount() {
        assertEquals(ArrayUtil.MAX_ARRAY_LENGTH, PromqlLogicalPlanBuilder.evaluationStepCount(0, ArrayUtil.MAX_ARRAY_LENGTH - 1L, 1));
        expectThrows(
            QlIllegalArgumentException.class,
            () -> PromqlLogicalPlanBuilder.evaluationStepCount(0, ArrayUtil.MAX_ARRAY_LENGTH, 1)
        );
        expectThrows(
            QlIllegalArgumentException.class,
            () -> PromqlLogicalPlanBuilder.evaluationStepCount(Long.MIN_VALUE, Long.MAX_VALUE, 1)
        );
        expectThrows(QlIllegalArgumentException.class, () -> PromqlLogicalPlanBuilder.evaluationStepCount(1, 0, 1));
        expectThrows(QlIllegalArgumentException.class, () -> PromqlLogicalPlanBuilder.evaluationStepCount(0, 1, 0));
    }
}
