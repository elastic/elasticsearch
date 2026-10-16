/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Equality (==)
public final class EqualsIntConstraint implements IntConstraint {
    private final int targetValue;

    EqualsIntConstraint(int targetValue) {
        this.targetValue = targetValue;
    }

    @Override
    public boolean isApplicable(int value) {
        return value == targetValue;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(targetValue, targetValue) };
    }
}
