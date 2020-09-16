/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.List;
import java.util.Objects;

public class StringScriptFieldPrefixQuery extends AbstractStringScriptFieldQuery {
    private final String prefix;

    public StringScriptFieldPrefixQuery(Script script, StringFieldScript.LeafFactory leafFactory, String fieldName, String prefix) {
        super(script, leafFactory, fieldName);
        this.prefix = Objects.requireNonNull(prefix);
    }

    @Override
    protected boolean matches(List<String> values) {
        for (String value : values) {
            if (value != null && value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.consumeTermsMatching(this, fieldName(), () -> new ByteRunAutomaton(PrefixQuery.toAutomaton(new BytesRef(prefix))));
        }
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return prefix + "*";
        }
        return fieldName() + ":" + prefix + "*";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldPrefixQuery other = (StringScriptFieldPrefixQuery) obj;
        return prefix.equals(other.prefix);
    }

    String prefix() {
        return prefix;
    }
}
