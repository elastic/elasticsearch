/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.List;
import java.util.Objects;

public class StringScriptFieldTermQuery extends AbstractStringScriptFieldQuery {
    private final String term;
    private final boolean caseInsensitive;

    public StringScriptFieldTermQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String term,
        boolean caseInsensitive
    ) {
        super(script, leafFactory, fieldName);
        this.term = Objects.requireNonNull(term);
        this.caseInsensitive = caseInsensitive;
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
            return term;
        }
        return fieldName() + ":" + term;
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
