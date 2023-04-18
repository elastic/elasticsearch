/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.Script;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class LongScriptFieldTermsQuery extends AbstractLongScriptFieldQuery {
    private final Set<Long> terms;

    public LongScriptFieldTermsQuery(
        Script script,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        String fieldName,
        Set<Long> terms
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

    Set<Long> terms() {
        return terms;
    }
}
