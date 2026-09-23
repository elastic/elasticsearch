/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Queries over binary doc values that carry a blob per document: every one of them is read and compared. The framing of
 * that blob still varies, so it is carried through to whichever reader decodes it.
 */
final class ScanningBinaryDocValuesQueries implements BinaryDocValuesQueries {

    /**
     * One per format, since the format is all that distinguishes them. A columnar field is answered by its column and
     * not by scanning, so there is nothing here for it and asking is a routing mistake.
     */
    private static final Map<BinaryDocValuesFormat, ScanningBinaryDocValuesQueries> BY_FORMAT = new EnumMap<>(
        Arrays.stream(BinaryDocValuesFormat.values())
            .filter(format -> format != BinaryDocValuesFormat.COLUMNAR_PAYLOAD)
            .collect(Collectors.toMap(Function.identity(), ScanningBinaryDocValuesQueries::new))
    );

    static ScanningBinaryDocValuesQueries forFormat(BinaryDocValuesFormat format) {
        final ScanningBinaryDocValuesQueries queries = BY_FORMAT.get(Objects.requireNonNull(format));
        if (queries == null) {
            throw new IllegalArgumentException("[" + format + "] is answered by the column, not by scanning");
        }
        return queries;
    }

    private final BinaryDocValuesFormat format;

    private ScanningBinaryDocValuesQueries(BinaryDocValuesFormat format) {
        this.format = Objects.requireNonNull(format);
    }

    @Override
    public Query term(String field, BytesRef term) {
        return new ScanningBinaryDocValuesTermQuery(field, term, format);
    }

    @Override
    public Query terms(String field, Collection<BytesRef> terms) {
        return new ScanningBinaryDocValuesTermInSetQuery(field, List.copyOf(terms), format);
    }

    @Override
    public Query range(String field, @Nullable BytesRef lower, @Nullable BytesRef upper, boolean includeLower, boolean includeUpper) {
        return new ScanningBinaryDocValuesRangeQuery(field, lower, upper, includeLower, includeUpper, format);
    }

    @Override
    public Query prefix(String field, String value, boolean caseInsensitive) {
        return new ScanningBinaryDocValuesPrefixQuery(field, value, caseInsensitive, format);
    }

    @Override
    public Query fuzzy(String field, String term, int maxEdits, int prefixLength, boolean transpositions) {
        return ScanningBinaryDocValuesAutomatonQuery.forFuzzy(field, term, maxEdits, prefixLength, transpositions, format);
    }

    @Override
    public Query caseInsensitiveTerm(String field, String value) {
        return ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(field, value, format);
    }

    @Override
    public Query wildcard(String field, String pattern, boolean caseInsensitive) {
        return ScanningBinaryDocValuesAutomatonQuery.forWildcard(field, pattern, caseInsensitive, format);
    }

    @Override
    public Query automaton(String field, Automaton automaton, String description) {
        return new ScanningBinaryDocValuesAutomatonQuery(field, automaton, format, description);
    }

    @Override
    public Query regexp(
        String field,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable CircuitBreaker breaker
    ) {
        return new ScanningBinaryDocValuesRegexpQuery(field, pattern, syntaxFlags, matchFlags, maxDeterminizedStates, format, breaker);
    }
}
