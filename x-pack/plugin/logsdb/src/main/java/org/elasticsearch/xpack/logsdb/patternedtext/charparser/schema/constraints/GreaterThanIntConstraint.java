/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

// Greater Than (>)
public final class GreaterThanIntConstraint implements IntConstraint {
    private final int threshold;

    public GreaterThanIntConstraint(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean isApplicable(int value) {
        return value > threshold;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(threshold + 1, Integer.MAX_VALUE) };
    }
}
