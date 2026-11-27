/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.runner;

import org.elasticsearch.core.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and removes the seed that {@code tests.iters} makes {@link com.carrotsearch.randomizedtesting.RandomizedRunner} add to the name
 * of every test: {@code testFoo {seed=[X:Y]}}, or {@code testFoo {p=a seed=[X:Y]}} for a test with parameters. {@code X} is the seed of
 * the suite and {@code Y} the seed of the iteration.
 */
public final class IterationSeeds {

    private static final Pattern SEED_IN_NAME = Pattern.compile(" ?seed=\\[([0-9A-Fa-f]+:[0-9A-Fa-f]+)\\]");

    private IterationSeeds() {}

    /**
     * The {@code suiteSeed:iterationSeed} in the name of a test, or {@code null} if it has none, which is the case unless the test is
     * repeated with {@code tests.iters}.
     */
    @Nullable
    public static String seedOf(@Nullable String testName) {
        if (testName == null) {
            return null;
        }
        Matcher matcher = SEED_IN_NAME.matcher(testName);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * The name of a test without its seed: {@code testFoo {p=a seed=[X:Y]}} is {@code testFoo {p=a}}, and {@code testFoo {seed=[X:Y]}}
     * is {@code testFoo}.
     */
    @Nullable
    public static String withoutSeed(@Nullable String testName) {
        if (testName == null) {
            return null;
        }
        return SEED_IN_NAME.matcher(testName).replaceAll("").replace(" {}", "");
    }
}
