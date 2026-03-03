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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

import java.io.IOException;
import java.util.Objects;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equivalent of {@link org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery}, but then without the scripting overhead and
 * just for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
// TODO: create abstract class for binary doc values based automaton queries in follow up, in order to support regex and fuzzy queries.
public final class SlowCustomBinaryDocValuesWildcardQuery extends AbstractBinaryDocValuesQuery {

    private final String pattern;
    private final boolean caseInsensitive;

    public SlowCustomBinaryDocValuesWildcardQuery(String fieldName, String pattern, boolean caseInsensitive) {
        this(fieldName, pattern, caseInsensitive, buildByteRunAutomaton(fieldName, pattern, caseInsensitive));
    }

    private SlowCustomBinaryDocValuesWildcardQuery(String fieldName, String pattern, boolean caseInsensitive, ByteRunAutomaton automaton) {
        super(fieldName, value -> automaton.run(value.bytes, value.offset, value.length));
        this.pattern = Objects.requireNonNull(pattern);
        this.caseInsensitive = caseInsensitive;
    }

    private static ByteRunAutomaton buildByteRunAutomaton(String fieldName, String pattern, boolean caseInsensitive) {
        Term term = new Term(Objects.requireNonNull(fieldName), Objects.requireNonNull(pattern));
        Automaton automaton;
        if (caseInsensitive) {
            automaton = AutomatonQueries.toCaseInsensitiveWildcardAutomaton(term);
        } else {
            automaton = WildcardQuery.toAutomaton(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        return new ByteRunAutomaton(automaton);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        var innerPattern = getContainsPattern(pattern);
        if (innerPattern != null) {
            return new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef(innerPattern));
        } else {
            return super.rewrite(indexSearcher);
        }
    }

    /**
     * The only patterns that can be re-written to a contains queries are of the form "*A*". The inner string cannot contain
     * a wildcard, ?, or whitespace. Return pattern directly, rather than a boolean, to avoid getting substring twice.
     * @param pattern the original wildcard pattern
     * @return the contain pattern
     */
    String getContainsPattern(String pattern) {
        // Must contain at least 3 characters to have a string surrounded by two wildcards
        if (pattern.length() < 3) {
            return null;
        }
        if (pattern.startsWith("*") == false || pattern.endsWith("*") == false) {
            return null;
        }
        var inner = pattern.substring(1, pattern.length() - 1);
        if (inner.contains("*")) {
            return null;
        }
        if (inner.contains("?")) {
            return null;
        }
        if (inner.contains(" ")) {
            return null;
        }
        return inner;
    }

    @Override
    protected float matchCost() {
        return 1000f; // This is just expensive, not sure what the actual cost is.
    }

    @Override
    public String toString(String field) {
        return "SlowCustomBinaryDocValuesWildcardQuery(fieldName="
            + field
            + ",pattern="
            + pattern
            + ",caseInsensitive="
            + caseInsensitive
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
        SlowCustomBinaryDocValuesWildcardQuery that = (SlowCustomBinaryDocValuesWildcardQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(pattern, that.pattern)
            && caseInsensitive == that.caseInsensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, pattern, caseInsensitive);
    }
}
