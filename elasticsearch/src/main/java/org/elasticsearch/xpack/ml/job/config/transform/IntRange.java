/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform;

import java.util.Objects;

public class IntRange {
    public enum BoundType {
        OPEN, CLOSED
    }

    public static class Bound {
        private final int value;
        private final BoundType boundType;

        public Bound(int value, BoundType boundType) {
            this.value = value;
            this.boundType = Objects.requireNonNull(boundType);
        }
    }

    private static String PLUS_INFINITY = "+\u221E";
    private static String MINUS_INFINITY = "-\u221E";
    private static char LEFT_BRACKET = '(';
    private static char RIGHT_BRACKET = ')';
    private static char LEFT_SQUARE_BRACKET = '[';
    private static char RIGHT_SQUARE_BRACKET = ']';
    private static char BOUNDS_SEPARATOR = '\u2025';

    private final Bound lower;
    private final Bound upper;

    private IntRange(Bound lower, Bound upper) {
        this.lower = Objects.requireNonNull(lower);
        this.upper = Objects.requireNonNull(upper);
    }

    public boolean contains(int value) {
        int lowerIncludedValue = lower.boundType == BoundType.CLOSED ? lower.value : lower.value + 1;
        int upperIncludedValue = upper.boundType == BoundType.CLOSED ? upper.value : upper.value - 1;
        return value >= lowerIncludedValue && value <= upperIncludedValue;
    }

    public boolean hasLowerBound() {
        return lower.value != Integer.MIN_VALUE;
    }

    public boolean hasUpperBound() {
        return upper.value != Integer.MAX_VALUE;
    }

    public int lower() {
        return lower.value;
    }

    public int upper() {
        return upper.value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(hasLowerBound() && lower.boundType == BoundType.CLOSED ? LEFT_SQUARE_BRACKET : LEFT_BRACKET);
        builder.append(hasLowerBound() ? lower.value : MINUS_INFINITY);
        builder.append(BOUNDS_SEPARATOR);
        builder.append(hasUpperBound() ? upper.value : PLUS_INFINITY);
        builder.append(hasUpperBound() && upper.boundType == BoundType.CLOSED ? RIGHT_SQUARE_BRACKET : RIGHT_BRACKET);
        return builder.toString();
    }

    public static IntRange singleton(int value) {
        return closed(value, value);
    }

    public static IntRange closed(int lower, int upper) {
        return new IntRange(closedBound(lower), closedBound(upper));
    }

    public static IntRange open(int lower, int upper) {
        return new IntRange(openBound(lower), openBound(upper));
    }

    public static IntRange openClosed(int lower, int upper) {
        return new IntRange(openBound(lower), closedBound(upper));
    }

    public static IntRange closedOpen(int lower, int upper) {
        return new IntRange(closedBound(lower), openBound(upper));
    }

    public static IntRange atLeast(int lower) {
        return closed(lower, Integer.MAX_VALUE);
    }

    private static Bound openBound(int value) {
        return new Bound(value, BoundType.OPEN);
    }

    private static Bound closedBound(int value) {
        return new Bound(value, BoundType.CLOSED);
    }
}
