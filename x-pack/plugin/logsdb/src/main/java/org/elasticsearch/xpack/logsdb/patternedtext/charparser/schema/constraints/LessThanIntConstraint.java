/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Less Than (<)
public final class LessThanIntConstraint implements IntConstraint {
    private final int threshold;

    public LessThanIntConstraint(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean isApplicable(int value) {
        return value < threshold;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, threshold - 1) };
    }
}
