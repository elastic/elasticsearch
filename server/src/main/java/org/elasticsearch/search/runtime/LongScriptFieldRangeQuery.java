/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class LongScriptFieldRangeQuery extends AbstractLongScriptFieldQuery {
    private final long lowerValue;
    private final long upperValue;

    public LongScriptFieldRangeQuery(
        Script script,
        String fieldName,
        Query approximation,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        long lowerValue,
        long upperValue
    ) {
        super(script, fieldName, approximation, leafFactory);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        assert lowerValue <= upperValue : lowerValue + " <= " + upperValue;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newApprox = approximation().rewrite(reader);
        if (newApprox == approximation()) {
            return this;
        }
        // TODO move rewrite up
        return new LongScriptFieldRangeQuery(script(), fieldName(), newApprox, scriptContextFunction(), lowerValue, upperValue);
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
        return b.toString() + " approximated by " + approximation();  // TODO move the toString enhancement into the superclass
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
