/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Length constraint ({})
public final class LengthIntConstraint implements IntConstraint {
    int lowerBound;
    int upperBound;

    public LengthIntConstraint(int length) {
        this.lowerBound = (int) Math.pow(10, length - 1);
        this.upperBound = (int) Math.pow(10, length) - 1;
    }

    @Override
    public boolean isApplicable(int value) {
        return value >= lowerBound && value <= upperBound;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(lowerBound, upperBound) };
    }
}
