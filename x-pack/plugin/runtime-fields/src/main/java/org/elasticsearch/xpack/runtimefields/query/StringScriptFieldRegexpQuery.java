/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.Objects;

public class StringScriptFieldRegexpQuery extends AbstractStringScriptFieldAutomatonQuery {
    private final String pattern;
    private final int flags;

    public StringScriptFieldRegexpQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String pattern,
        int flags,
        int maxDeterminizedStates
    ) {
        super(
            script,
            leafFactory,
            fieldName,
            new ByteRunAutomaton(new RegExp(Objects.requireNonNull(pattern), flags).toAutomaton(maxDeterminizedStates))
        );
        this.pattern = pattern;
        this.flags = flags;
    }

    @Override
    public final String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().contentEquals(field)) {
            b.append(fieldName()).append(':');
        }
        return b.append('/').append(pattern).append('/').toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern, flags);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldRegexpQuery other = (StringScriptFieldRegexpQuery) obj;
        return pattern.equals(other.pattern) && flags == other.flags;
    }

    String pattern() {
        return pattern;
    }

    int flags() {
        return flags;
    }
}
