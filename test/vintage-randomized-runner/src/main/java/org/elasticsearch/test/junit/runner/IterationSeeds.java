/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.runner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and removes the seed that {@code tests.iters} makes {@link com.carrotsearch.randomizedtesting.RandomizedRunner} add to the name
 * of every test: {@code testFoo {seed=[X:Y]}}, or {@code testFoo {p=a seed=[X:Y]}} for a test with parameters. {@code X} is the seed of
 * the suite and {@code Y} the seed of the iteration.
 */
public final class IterationSeeds {

    /** The seed the runner adds as the last parameter of a test, and the brace that closes the parameters. */
    private static final Pattern TRAILING_SEED = Pattern.compile(" ?seed=\\[([0-9A-Fa-f]+:[0-9A-Fa-f]+)\\](?=\\}$)");

    /** The parameters the runner leaves of a test without any: they only held the seed. */
    private static final String EMPTY_PARAMETERS = " {}";

    private IterationSeeds() {}

    /**
     * The {@code suiteSeed:iterationSeed} in the name of a test, or {@code null} if it has none, which is the case unless the test is
     * repeated with {@code tests.iters}.
     */
    public static String seedOf(String testName) {
        if (testName == null) {
            return null;
        }
        Matcher matcher = TRAILING_SEED.matcher(testName);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * The name of a test without its seed: {@code testFoo {p=a seed=[X:Y]}} is {@code testFoo {p=a}}, and {@code testFoo {seed=[X:Y]}}
     * is {@code testFoo}. A name that does not end with a seed is returned as it is, including parameters that are empty or that
     * contain braces, such as {@code testFoo {q=match {}}}.
     */
    public static String withoutSeed(String testName) {
        if (testName == null) {
            return null;
        }
        Matcher matcher = TRAILING_SEED.matcher(testName);
        if (matcher.find() == false) {
            return testName;
        }
        String withoutSeed = testName.substring(0, matcher.start()) + testName.substring(matcher.end());
        return withoutSeed.endsWith(EMPTY_PARAMETERS)
            ? withoutSeed.substring(0, withoutSeed.length() - EMPTY_PARAMETERS.length())
            : withoutSeed;
    }
}
