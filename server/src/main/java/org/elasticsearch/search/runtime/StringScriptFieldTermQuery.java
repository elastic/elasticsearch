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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class StringScriptFieldTermQuery extends AbstractStringScriptFieldQuery {
    private final String term;
    private final boolean caseInsensitive;

    public StringScriptFieldTermQuery(
        Script script,
        String fieldName,
        Query approximation,
        Function<LeafReaderContext, StringFieldScript> leafFactory,
        String term,
        boolean caseInsensitive
    ) {
        super(script, fieldName, approximation, leafFactory);
        this.term = Objects.requireNonNull(term);
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newApprox = approximation().rewrite(reader);
        if (newApprox == approximation()) {
            return this;
        }
        // TODO move rewrite up
        return new StringScriptFieldTermQuery(script(), fieldName(), newApprox, scriptContextFunction(), term, caseInsensitive);
    }

    @Override
    protected boolean matches(List<String> values) {
        for (String value : values) {
            if (caseInsensitive) {
                if (term.equalsIgnoreCase(value)) {
                    return true;
                }
            } else if (term.equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.consumeTerms(this, new Term(fieldName(), term));
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return term + " approximated by " + approximation();  // TODO move the toString enhancement into the superclass
        }
        return fieldName() + ":" + term + " approximated by " + approximation();  // TODO move the toString enhancement into the superclass
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldTermQuery other = (StringScriptFieldTermQuery) obj;
        return term.equals(other.term) && caseInsensitive == other.caseInsensitive;
    }

    String term() {
        return term;
    }

    boolean caseInsensitive() {
        return caseInsensitive;
    }
}
