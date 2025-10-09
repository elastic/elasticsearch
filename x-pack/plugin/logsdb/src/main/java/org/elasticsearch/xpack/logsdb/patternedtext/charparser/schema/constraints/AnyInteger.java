/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

/**
 * No constraint - all values are valid.
 */
public final class AnyInteger implements IntConstraint {
    public static final IntConstraint INSTANCE = new AnyInteger();

    private AnyInteger() {}

    @Override
    public boolean isApplicable(int value) {
        return true;
    }

    @Override
    public IntConstraints.Range[] trueRanges() {
        return new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, Integer.MAX_VALUE) };
    }
}
