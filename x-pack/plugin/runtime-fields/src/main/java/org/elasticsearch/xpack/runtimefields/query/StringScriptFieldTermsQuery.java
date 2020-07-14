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
import java.util.Set;

public class StringScriptFieldTermsQuery extends AbstractStringScriptFieldQuery {
    private final Set<String> terms;

    public StringScriptFieldTermsQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName, Set<String> terms) {
        super(leafFactory, fieldName);
        this.terms = terms;
    }

    @Override
    public boolean matches(List<String> values) {
        for (String value : values) {
            if (terms.contains(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            for (String term : terms) {
                visitor.consumeTerms(this, new Term(fieldName(), term));
            }
        }
    }

    @Override
    public String bareToString() {
        return terms.toString();
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
        StringScriptFieldTermsQuery other = (StringScriptFieldTermsQuery) obj;
        return other.terms.equals(other.terms);
    }
}
