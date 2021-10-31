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

public class LongScriptFieldTermQuery extends AbstractLongScriptFieldQuery {
    private final long term;

    public LongScriptFieldTermQuery(
        Script script,
        String fieldName,
        Query approximation,
        Function<LeafReaderContext, AbstractLongFieldScript> leafFactory,
        long term
    ) {
        super(script, fieldName, approximation, leafFactory);
        this.term = term;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newApprox = approximation().rewrite(reader);
        if (newApprox == approximation()) {
            return this;
        }
        // TODO move rewrite up
        return new LongScriptFieldTermQuery(script(), fieldName(), newApprox, scriptContextFunction(), term);
    }

    @Override
    protected boolean matches(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (term == values[i]) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return Long.toString(term) + " approximated by " + approximation();  // TODO move the toString enhancement into the superclass
        }
        return fieldName() + ":" + term + " approximated by " + approximation();  // TODO move the toString enhancement into the superclass
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        LongScriptFieldTermQuery other = (LongScriptFieldTermQuery) obj;
        return term == other.term;
    }

    long term() {
        return term;
    }
}
