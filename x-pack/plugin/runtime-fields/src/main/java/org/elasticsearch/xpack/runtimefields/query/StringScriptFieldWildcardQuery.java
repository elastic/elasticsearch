/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.Objects;

public class StringScriptFieldWildcardQuery extends AbstractStringScriptFieldAutomatonQuery {
    private final String pattern;

    public StringScriptFieldWildcardQuery(Script script, StringFieldScript.LeafFactory leafFactory, String fieldName, String pattern) {
        super(
            script,
            leafFactory,
            fieldName,
            new ByteRunAutomaton(WildcardQuery.toAutomaton(new Term(fieldName, Objects.requireNonNull(pattern))))
        );
        this.pattern = pattern;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().equals(field)) {
            return pattern;
        }
        return fieldName() + ":" + pattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldWildcardQuery other = (StringScriptFieldWildcardQuery) obj;
        return pattern.equals(other.pattern);
    }

    String pattern() {
        return pattern;
    }
}
