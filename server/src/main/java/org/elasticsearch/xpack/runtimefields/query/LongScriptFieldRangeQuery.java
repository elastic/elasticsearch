/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.AbstractLongFieldScript;

import java.util.Objects;
import java.util.function.Function;

public class LongScriptFieldRangeQuery extends AbstractLongScriptFieldQuery {
    private final long lowerValue;
    private final long upperValue;

    public LongScriptFieldRangeQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        String fieldName,
        long lowerValue,
        long upperValue
    ) {
        super(script, leafFactory, fieldName);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        assert lowerValue <= upperValue;
    }

    @Override
    protected boolean matches(long[] values, int count) {
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
        LongScriptFieldRangeQuery other = (LongScriptFieldRangeQuery) obj;
        return lowerValue == other.lowerValue && upperValue == other.upperValue;
    }

    long lowerValue() {
        return lowerValue;
    }

    long upperValue() {
        return upperValue;
    }
}
