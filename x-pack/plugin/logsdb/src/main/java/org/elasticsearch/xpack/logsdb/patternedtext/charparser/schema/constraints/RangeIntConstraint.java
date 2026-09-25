/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Inclusive Range (-)
public final class RangeIntConstraint implements IntConstraint {
    private final int lowerBound;
    private final int upperBound;

    public RangeIntConstraint(int lowerBound, int upperBound) {
        if (lowerBound > upperBound) {
            throw new IllegalArgumentException("Lower bound cannot be greater than upper bound in range constraint");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public boolean isApplicable(int value) {
        return value >= lowerBound && value <= upperBound;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        if (lowerBound > upperBound) {
            throw new IllegalArgumentException("Lower bound cannot be greater than upper bound in range constraint");
        }
        return new IntConstraints.Range[] { new IntConstraints.Range(lowerBound, upperBound) };
    }
}
