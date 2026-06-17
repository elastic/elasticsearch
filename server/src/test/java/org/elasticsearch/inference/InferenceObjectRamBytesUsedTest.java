/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

/**
 * {@link InferenceObjectRamBytesUsedTest} tests, whether objects tracked by the inference {@link org.elasticsearch.common.breaker.CircuitBreaker}
 * implement a sensible {@link Accountable#ramBytesUsed()} estimation by checking the following invariants:
 * - Each object's bytes estimation should be larger than 0
 * - Objects with more and/or larger inputs should have a higher estimation than an object with fewer and/or smaller inputs
 */
public abstract class InferenceObjectRamBytesUsedTest<T extends Accountable> extends ESTestCase {

    public abstract T objectToEstimate();

    public abstract List<T> objectsToEstimateWithLargerInput();

    public boolean hasGrowingInputs() {
        return true;
    }

    public void testRamBytesUsed_IsPositive() {
        assertThat(objectToEstimate().ramBytesUsed(), greaterThan(0L));
    }

    public void testRamBytesUsed_GrowsWithLargerInputs() {
        assumeTrue(
            "testRamBytesUsed_GrowsWithLargerInputs() is skipped for objects, which only have constant-size fields",
            hasGrowingInputs()
        );
        for (T obj : objectsToEstimateWithLargerInput()) {
            assertThat(obj.ramBytesUsed(), greaterThan(objectToEstimate().ramBytesUsed()));
        }
    }
}
