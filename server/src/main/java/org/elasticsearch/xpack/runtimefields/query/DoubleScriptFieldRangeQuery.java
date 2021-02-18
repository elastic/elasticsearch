/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleFieldScript;

import java.util.Objects;

public class DoubleScriptFieldRangeQuery extends AbstractDoubleScriptFieldQuery {
    private final double lowerValue;
    private final double upperValue;

    public DoubleScriptFieldRangeQuery(
        Script script,
        DoubleFieldScript.LeafFactory leafFactory,
        String fieldName,
        double lowerValue,
        double upperValue
    ) {
        super(script, leafFactory, fieldName);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        assert lowerValue <= upperValue;
    }

    @Override
    protected boolean matches(double[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (lowerValue <= values[i] && values[i] <= upperValue) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().contentEquals(field)) {
            b.append(fieldName()).append(':');
        }
        b.append('[').append(lowerValue).append(" TO ").append(upperValue).append(']');
        return b.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lowerValue, upperValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        DoubleScriptFieldRangeQuery other = (DoubleScriptFieldRangeQuery) obj;
        return lowerValue == other.lowerValue && upperValue == other.upperValue;
    }

    double lowerValue() {
        return lowerValue;
    }

    double upperValue() {
        return upperValue;
    }
}
