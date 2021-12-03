/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.Script;

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
            if (Double.compare(lowerValue, values[i]) <= 0 && Double.compare(values[i], upperValue) <= 0) {
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
