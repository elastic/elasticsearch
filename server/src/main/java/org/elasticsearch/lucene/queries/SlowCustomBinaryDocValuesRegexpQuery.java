/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import java.util.Objects;

/**
 * A query that matches documents where a binary doc values field contains a value matching the given regular expression.
 * The equivalent of {@link org.elasticsearch.search.runtime.StringScriptFieldRegexpQuery}, but without the scripting overhead and
 * just for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesRegexpQuery extends AbstractBinaryDocValuesQuery {

    private final String pattern;
    private final int syntaxFlags;
    private final int matchFlags;
    private final int maxDeterminizedStates;

    public SlowCustomBinaryDocValuesRegexpQuery(
        String fieldName,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates
    ) {
        this(
            fieldName,
            pattern,
            syntaxFlags,
            matchFlags,
            maxDeterminizedStates,
            buildAutomaton(pattern, syntaxFlags, matchFlags, maxDeterminizedStates)
        );
    }

    private SlowCustomBinaryDocValuesRegexpQuery(
        String fieldName,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        ByteRunAutomaton automaton
    ) {
        super(fieldName, value -> automaton.run(value.bytes, value.offset, value.length));
        this.pattern = Objects.requireNonNull(pattern);
        this.syntaxFlags = syntaxFlags;
        this.matchFlags = matchFlags;
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    private static ByteRunAutomaton buildAutomaton(String pattern, int syntaxFlags, int matchFlags, int maxDeterminizedStates) {
        return new ByteRunAutomaton(
            Operations.determinize(
                new RegExp(Objects.requireNonNull(pattern), syntaxFlags, matchFlags).toAutomaton(),
                maxDeterminizedStates
            )
        );
    }

    @Override
    protected float matchCost() {
        return 1000f;
    }

    @Override
    public String toString(String field) {
        return "SlowCustomBinaryDocValuesRegexpQuery(fieldName="
            + field
            + ",pattern=/"
            + pattern
            + "/,syntaxFlags="
            + syntaxFlags
            + ",matchFlags="
            + matchFlags
            + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesRegexpQuery that = (SlowCustomBinaryDocValuesRegexpQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(pattern, that.pattern)
            && syntaxFlags == that.syntaxFlags
            && matchFlags == that.matchFlags
            && maxDeterminizedStates == that.maxDeterminizedStates;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
    }
}
