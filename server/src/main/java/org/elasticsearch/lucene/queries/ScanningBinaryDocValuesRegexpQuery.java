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
public final class ScanningBinaryDocValuesRegexpQuery extends AbstractBinaryDocValuesAutomatonQuery {

    private final String pattern;
    private final int syntaxFlags;
    private final int matchFlags;
    private final int maxDeterminizedStates;

    /**
     * @param pattern the regexp pattern to match; callers must pre-process it with
     *                {@link org.elasticsearch.common.lucene.search.AutomatonQueries#collapseConsecutiveQuantifiers}
     *                to avoid determinization complexity blowup on patterns like {@code a**}.
     */
    public ScanningBinaryDocValuesRegexpQuery(
        String fieldName,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        boolean arrayOrderInlineNull
    ) {
        super(fieldName, buildAutomaton(pattern, syntaxFlags, matchFlags, maxDeterminizedStates), arrayOrderInlineNull);
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
    public String toString(String field) {
        return "ScanningBinaryDocValuesRegexpQuery(fieldName="
            + fieldName
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
        ScanningBinaryDocValuesRegexpQuery that = (ScanningBinaryDocValuesRegexpQuery) o;
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
