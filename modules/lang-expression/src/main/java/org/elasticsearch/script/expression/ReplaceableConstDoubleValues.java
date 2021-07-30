/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.search.DoubleValues;

/**
 * A support class for an executable expression script that allows the double returned
 * by a {@link DoubleValues} to be modified.
 */
final class ReplaceableConstDoubleValues extends DoubleValues {
    private double value = 0;

    void setValue(double value) {
        this.value = value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public boolean advanceExact(int doc) {
        return true;
    }
}
