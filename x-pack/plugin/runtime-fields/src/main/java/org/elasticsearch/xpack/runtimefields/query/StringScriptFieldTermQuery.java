/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.util.List;
import java.util.Objects;

public class StringScriptFieldTermQuery extends AbstractStringScriptFieldQuery {
    private final String term;

    public StringScriptFieldTermQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName, String term) {
        super(leafFactory, fieldName);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public boolean matches(List<String> values) {
        for (String value : values) {
            if (term.equals(value)) {
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
        return Objects.hash(super.hashCode(), term);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldTermQuery other = (StringScriptFieldTermQuery) obj;
        return other.term.equals(other.term);
    }

    String term() {
        return term;
    }
}
