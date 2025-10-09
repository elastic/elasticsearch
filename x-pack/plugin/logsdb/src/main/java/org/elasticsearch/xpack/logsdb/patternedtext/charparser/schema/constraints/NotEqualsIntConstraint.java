/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Not Equal (!=)
public final class NotEqualsIntConstraint implements IntConstraint {
    private final int targetValue;

    public NotEqualsIntConstraint(int targetValue) {
        this.targetValue = targetValue;
    }

    @Override
    public boolean isApplicable(int value) {
        return value != targetValue;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] {
            new IntConstraints.Range(Integer.MIN_VALUE, targetValue - 1),
            new IntConstraints.Range(targetValue + 1, Integer.MAX_VALUE) };
    }
}
