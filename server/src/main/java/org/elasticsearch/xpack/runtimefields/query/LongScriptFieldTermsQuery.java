/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.AbstractLongFieldScript;

import java.util.Objects;
import java.util.function.Function;

public class LongScriptFieldTermsQuery extends AbstractLongScriptFieldQuery {
    private final LongSet terms;

    public LongScriptFieldTermsQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        String fieldName,
        LongSet terms
    ) {
        super(script, leafFactory, fieldName);
        this.terms = terms;
    }

    @Override
    protected boolean matches(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (terms.contains(values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return terms.toString();
        }
        return fieldName() + ":" + terms;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), terms);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        LongScriptFieldTermsQuery other = (LongScriptFieldTermsQuery) obj;
        return terms.equals(other.terms);
    }

    LongSet terms() {
        return terms;
    }
}
