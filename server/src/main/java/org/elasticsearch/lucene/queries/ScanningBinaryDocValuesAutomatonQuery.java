/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

import java.util.Objects;

/**
 * A query that matches documents where a binary doc values field contains a value matching the given automaton.
 * The equivalent of {@link org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery}, but then without the scripting overhead and
 * just for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class ScanningBinaryDocValuesAutomatonQuery extends AbstractBinaryDocValuesAutomatonQuery {

    private final String description;

    public ScanningBinaryDocValuesAutomatonQuery(String fieldName, Automaton automaton, boolean arrayOrderInlineNull, String description) {
        super(fieldName, new ByteRunAutomaton(automaton), arrayOrderInlineNull);
        this.description = Objects.requireNonNull(description);
    }

    /** Creates a query matching a wildcard pattern, rewriting {@code *literal*} patterns to a faster contains query. */
    public static Query forWildcard(String fieldName, String pattern, boolean caseInsensitive, boolean arrayOrderInlineNull) {
        if (caseInsensitive == false) {
            var innerPattern = getContainsPattern(pattern);
            if (innerPattern != null) {
                return new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef(innerPattern), arrayOrderInlineNull);
            }
        }
        return new ScanningBinaryDocValuesAutomatonQuery(
            fieldName,
            buildAutomaton(fieldName, pattern, caseInsensitive),
            arrayOrderInlineNull,
            "pattern=" + pattern + ",caseInsensitive=" + caseInsensitive
        );
    }

    private static Automaton buildAutomaton(String fieldName, String pattern, boolean caseInsensitive) {
        Term term = new Term(Objects.requireNonNull(fieldName), Objects.requireNonNull(pattern));
        Automaton automaton;
        if (caseInsensitive) {
            automaton = AutomatonQueries.toCaseInsensitiveWildcardAutomaton(term);
        } else {
            automaton = WildcardQuery.toAutomaton(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        return automaton;
    }

    /**
     * Extracts the inner literal from patterns of the form {@code *literal*} that can be rewritten to a contains query.
     * Returns {@code null} if the pattern cannot be rewritten (contains wildcards, single-char wildcards, or escapes).
     * Backslash-containing patterns are rejected because wildcard syntax uses {@code \} to escape special characters
     * (e.g. {@code \*} for a literal asterisk), so the raw pattern string doesn't match the intended literal bytes.
     */
    static String getContainsPattern(String pattern) {
        if (pattern.length() < 3) {
            return null;
        }
        if (pattern.charAt(0) != '*' || pattern.charAt(pattern.length() - 1) != '*') {
            return null;
        }
        var inner = pattern.substring(1, pattern.length() - 1);
        if (inner.indexOf('*') >= 0 || inner.indexOf('?') >= 0 || inner.indexOf('\\') >= 0) {
            return null;
        }
        return inner;
    }

    @Override
    public String toString(String field) {
        return "ScanningBinaryDocValuesAutomatonQuery(fieldName=" + fieldName + "," + description + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesAutomatonQuery that = (ScanningBinaryDocValuesAutomatonQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(automaton, that.automaton)
            && arrayOrderInlineNull == that.arrayOrderInlineNull;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, automaton, arrayOrderInlineNull);
    }
}
