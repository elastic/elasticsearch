/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

/**
 * Character-length constraint ({@code {n}}) for a numeric subToken: the subToken must be exactly {@code n} characters long. This is a
 * property of the raw text (so leading zeros count), NOT of the value, and is enforced by the parser's char-length gate. The constraint is
 * therefore value-NEUTRAL - {@link #isApplicable} is always true and {@link #trueRanges} is the full range - so that when it is combined
 * with a value constraint via {@link #and}, it never clips the value range (a value floor, e.g. rejecting negatives, is a base-type
 * property, applied by the compiler). It only reports the required length via {@link #getRequiredCharLength}.
 */
public final class LengthIntConstraint implements IntConstraint {
    private final int length;

    public LengthIntConstraint(int length) {
        this.length = length;
    }

    @Override
    public boolean isApplicable(int value) {
        return true;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, Integer.MAX_VALUE) };
    }

    @Override
    public int getRequiredCharLength() {
        return length;
    }
}
