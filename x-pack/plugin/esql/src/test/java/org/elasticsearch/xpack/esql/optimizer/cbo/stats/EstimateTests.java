/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.test.ESTestCase;

public class EstimateTests extends ESTestCase {
    public void testUnknownEstimateIsConservative() {
        assertTrue(Estimate.UNKNOWN.isUnknown());
        assertEquals(0, Estimate.UNKNOWN.probabilityAtMost(1_000), 0);
    }

    public void testExactEstimateHasNoUncertainty() {
        Estimate estimate = Estimate.exact(10);

        assertEquals(10, estimate.valueOr(-1), 0);
        assertEquals(1, estimate.probabilityAtMost(10), 0);
        assertEquals(0, estimate.probabilityAtMost(9), 0);
    }

    public void testSampledEstimateRetainsAProbabilityRange() {
        Estimate estimate = Estimate.sampled(100, 10);

        assertTrue(estimate.probabilityAtMost(100) > 0);
        assertTrue(estimate.probabilityAtMost(100) < 1);
        assertEquals(1, estimate.probabilityAtMost(120), 0);
    }

    public void testArithmeticCombinesProvenanceAndBounds() {
        Estimate result = Estimate.metadata(100).multiply(Estimate.defaultValue(4));

        assertEquals(400, result.value(), 0);
        assertEquals(800, result.upperBound(), 0);
        assertEquals(Provenance.DEFAULT, result.provenance());
    }

    public void testDefinitelyZeroTimesUnknownIsKnownZero() {
        Estimate result = Estimate.exact(0).multiply(Estimate.UNKNOWN);

        assertEquals(0, result.value(), 0);
        assertEquals(0, result.upperBound(), 0);
        assertEquals(Provenance.EXACT, result.provenance());
    }
}
