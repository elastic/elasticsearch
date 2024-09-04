/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matches a value that is within given range (currently 1%) of an expected value.
 *
 * We need this because histograms do not store exact values, but only value ranges.
 * Since we have 2 significant digits, the value should be within 1% of the expected value.
 */
public class ApproximateMatcher extends TypeSafeMatcher<Long> {
    public static double ACCURACY = 0.01;
    private final long expectedValue;

    public ApproximateMatcher(long expectedValue) {
        this.expectedValue = expectedValue;
    }

    @Override
    protected boolean matchesSafely(Long actualValue) {
        double lowerBound = Math.floor(expectedValue * (1.00 - ACCURACY));
        double upperBound = Math.ceil(expectedValue * (1.00 + ACCURACY));
        return actualValue >= lowerBound && actualValue <= upperBound;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a long value within 1% of ").appendValue(expectedValue);
    }

    /**
     * Matches a value that is within given range (currently 1%) of an expected value.
     */
    public static ApproximateMatcher closeTo(long expectedValue) {
        return new ApproximateMatcher(expectedValue);
    }
}
