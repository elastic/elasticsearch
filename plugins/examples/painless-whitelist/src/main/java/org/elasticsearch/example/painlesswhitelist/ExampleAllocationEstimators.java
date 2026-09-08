/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.example.painlesswhitelist;

/**
 * An example of estimating what an allowlisted method allocates, so the Painless allocation limit and its metrics can
 * account for it. Referenced from {@code example_whitelist.txt} by the {@code @allocates} annotation.
 *
 * <p>An estimator is a {@code public static long} method whose parameters match the allowlisted method's, receiver first
 * for an instance method. It runs immediately before the real call, so it must be cheap, must not allocate, must not
 * throw, and must not consume anything it is handed.
 *
 * <p>This class is deliberately <b>not</b> allowlisted. A plugin's estimator has to be reachable from the generated
 * script's class loader without being visible to scripts, which is what {@code 40_allocation.yml} covers.
 */
public final class ExampleAllocationEstimators {

    private ExampleAllocationEstimators() {}

    /** Bytes {@link ExampleWhitelistedClass#repeat(int)} allocates: a String of {@code count} chars, 2 bytes each. */
    public static long repeatBytes(ExampleWhitelistedClass receiver, int count) {
        return stringBytes(count);
    }

    /**
     * Bytes {@link ExampleWhitelistedClass#staticRepeat(int)} allocates. The annotated method is static, so the estimator
     * takes only its arguments; an instance method's estimator takes the receiver first.
     */
    public static long staticRepeatBytes(int count) {
        return stringBytes(count);
    }

    private static long stringBytes(int chars) {
        return 32 + 2 * Math.max(0L, (long) chars);
    }
}
