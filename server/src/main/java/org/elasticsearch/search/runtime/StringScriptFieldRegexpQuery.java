/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;

import java.util.Objects;

public class StringScriptFieldRegexpQuery extends AbstractStringScriptFieldAutomatonQuery {
    private final String pattern;
    private final int syntaxFlags;
    private final int matchFlags;

    public StringScriptFieldRegexpQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates
    ) {
        super(
            script,
            leafFactory,
            fieldName,
            new ByteRunAutomaton(new RegExp(Objects.requireNonNull(pattern), syntaxFlags, matchFlags).toAutomaton(maxDeterminizedStates))
        );
        this.pattern = pattern;
        this.syntaxFlags = syntaxFlags;
        this.matchFlags = matchFlags;
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
        return Objects.hash(super.hashCode(), pattern, syntaxFlags, matchFlags);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldRegexpQuery other = (StringScriptFieldRegexpQuery) obj;
        return pattern.equals(other.pattern) && syntaxFlags == other.syntaxFlags && matchFlags == other.matchFlags;
    }

    String pattern() {
        return pattern;
    }

    int syntaxFlags() {
        return syntaxFlags;
    }

    int matchFlags() {
        return matchFlags;
    }
}
