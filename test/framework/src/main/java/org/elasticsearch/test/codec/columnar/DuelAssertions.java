/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The assertions a {@link BehaviorCheck} compares a baseline response against a contender response with. Each
 * check calls exactly one, chosen for the contract of the surface it reads: {@link #assertEquals} for results
 * whose element order is part of the contract (sort), and {@link #assertSameElements} for the set-like keyword
 * results whose order and duplicate multiplicity are not (document-membership sets, values, retrieval). A
 * failure names the contract that was violated.
 */
public final class DuelAssertions {

    private DuelAssertions() {}

    /**
     * Asserts the two lists are equal, honoring element order and duplicates.
     */
    public static void assertEquals(final String context, final List<?> expected, final List<?> actual) {
        if (expected.equals(actual) == false) {
            throw failure("equals", context, expected, actual);
        }
    }

    /**
     * Asserts the two lists have the same distinct elements, ignoring order and duplicate multiplicity.
     */
    public static void assertSameElements(final String context, final List<?> expected, final List<?> actual) {
        if (new HashSet<>(expected).equals(new HashSet<>(actual)) == false) {
            throw failure("same_elements", context, expected, actual);
        }
    }

    /**
     * Asserts the baseline and contender responses cover exactly the expected keys, so a missing or extra
     * per-key row cannot pass unnoticed in a downstream per-key comparison.
     */
    public static void assertSameKeys(final String context, final Set<?> expected, final Set<?> baseline, final Set<?> contender) {
        if (baseline.equals(expected) == false) {
            throw new AssertionError(context + " comparison=[keys] stage=[baseline] expected=" + expected + " actual=" + baseline);
        }
        if (contender.equals(expected) == false) {
            throw new AssertionError(context + " comparison=[keys] stage=[contender] expected=" + expected + " actual=" + contender);
        }
    }

    /**
     * Asserts two aggregation bucket maps are equal by key and count.
     */
    public static void assertEqualBuckets(final String context, final Map<String, Long> expected, final Map<String, Long> actual) {
        if (expected.equals(actual) == false) {
            throw new AssertionError(
                context + " comparison=[buckets] expected=" + new TreeMap<>(expected) + " actual=" + new TreeMap<>(actual)
            );
        }
    }

    /**
     * Asserts two counts are equal.
     */
    public static void assertCount(final String context, long expected, long actual) {
        if (expected != actual) {
            throw failure("count", context, expected, actual);
        }
    }

    private static AssertionError failure(final String name, final String context, final Object expected, final Object actual) {
        return new AssertionError(context + " comparison=[" + name + "] expected=" + expected + " actual=" + actual);
    }
}
