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
 * An example of estimating what an allowlisted method allocates, so the Painless allocation limit and metrics can account
 * for it. {@code example_whitelist.txt} points at these with {@code @allocates}.
 *
 * <p>An estimator is a {@code public static long} method whose parameters match the allowlisted method's, receiver first
 * for an instance method. It runs just before the real call, so it must be cheap and must not allocate, throw, or consume
 * what it is given.
 *
 * <p>This class is deliberately not allowlisted, so scripts cannot see it. See {@code 50_allocation.yml}.
 */
public final class ExampleAllocationEstimators {

    private ExampleAllocationEstimators() {}

    /** Bytes {@link ExampleWhitelistedClass#repeat(int)} allocates: a String of {@code count} chars. */
    public static long repeatBytes(ExampleWhitelistedClass receiver, int count) {
        return stringBytes(count);
    }

    /** Bytes {@link ExampleWhitelistedClass#staticRepeat(int)} allocates. Static, so no receiver. */
    public static long staticRepeatBytes(int count) {
        return stringBytes(count);
    }

    private static long stringBytes(int chars) {
        return 32 + 2 * Math.max(0L, (long) chars);
    }
}
