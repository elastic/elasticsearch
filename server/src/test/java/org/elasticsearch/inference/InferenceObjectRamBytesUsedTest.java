/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * {@link InferenceObjectRamBytesUsedTest} tests, whether objects tracked by the inference {@link org.elasticsearch.common.breaker.CircuitBreaker}
 * implement a sensible {@link Accountable#ramBytesUsed()} estimation by checking the following invariants:
 * - Each object's bytes estimation should be larger than 0
 * - Objects with more and/or larger inputs should have a higher estimation than an object with fewer and/or smaller inputs
 * - Objects should not "under account" meaning their estimate should not be smaller than what the deep object graph
 *   traversal of {@link RamUsageTester} returns.
 */
public abstract class InferenceObjectRamBytesUsedTest<T extends Accountable> extends ESTestCase {

    /**
     * Returns an object implementing {@link Accountable}.
     *
     * @return object of type T, which RAM should be estimated
     */
    public abstract T objectToEstimate();

    /**
     * Returns a list of objects implementing {@link Accountable}.
     * These objects should all be larger than the object returned by {@link #objectToEstimate()}
     * by increasing the size of the dominant field(s), e.g. an input text string.
     *
     * @return objects of type T, which RAM should be estimated
     */
    public abstract List<T> objectsToEstimateWithLargerInput();

    /**
     * Some objects implementing {@link Accountable} do not have growing inputs (usually user-provided input).
     * <code>return false;</code> from this method, if you want to skip {@link #testRamBytesUsed_GrowsWithLargerInputs()}.
     *
     * @return boolean, whether the object has growing inputs
     */
    public boolean hasGrowingInputs() {
        return true;
    }

    /**
     * The implementation of {@link Accountable#ramBytesUsed()} in {@link #objectToEstimate()}
     * should exceed {@link RamUsageTester#ramUsed(Object)} to make sure that we do not under account. Test can be skipped, if needed.
     *
     * @return boolean, specifying, whether {@link #testRamBytesUsed_DoesNotUnderAccount()} should be executed or not.
     */
    public boolean checkDoNotUnderAccount() {
        return true;
    }

    public void testRamBytesUsed_DoesNotUnderAccount() {
        assumeTrue(
            "testRamBytesUsed_DoesNotUnderAccount is skipped, "
                + "as the difference between the object's ramBytesUsed() implementation and "
                + "the return value of RamUsageTester.ramUsed(...) is negligible and cannot be meaningfully captured in the object's ramBytesUsed() implementation",
            checkDoNotUnderAccount()
        );
        assertThat(objectToEstimate().ramBytesUsed(), greaterThanOrEqualTo(RamUsageTester.ramUsed(objectToEstimate())));
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
