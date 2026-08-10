/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.core.Nullable;

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
     * @param circuitBreaker the request circuit breaker to account the automaton construction against; may be
     *                {@code null}, in which case the automaton is built without accounting (used by callers that
     *                have no breaker in hand, e.g. some unit tests).
     */
    public ScanningBinaryDocValuesRegexpQuery(
        String fieldName,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        boolean arrayOrderInlineNull,
        @Nullable CircuitBreaker circuitBreaker
    ) {
        super(
            fieldName,
            AutomatonQueries.toRegexpByteRunAutomaton(
                fieldName,
                Objects.requireNonNull(pattern),
                syntaxFlags,
                matchFlags,
                maxDeterminizedStates,
                circuitBreaker
            ),
            arrayOrderInlineNull
        );
        this.pattern = Objects.requireNonNull(pattern);
        this.syntaxFlags = syntaxFlags;
        this.matchFlags = matchFlags;
        this.maxDeterminizedStates = maxDeterminizedStates;
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
