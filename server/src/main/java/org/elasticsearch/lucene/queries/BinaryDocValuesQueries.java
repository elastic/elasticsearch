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

import java.util.Collection;

/**
 * The queries a field answers from its binary doc values, for a field with no inverted index to answer them from.
 *
 * <p>How a document's values are framed on disk decides how a query over them is best answered, and the two are not the
 * same question at every shape. A field whose doc values are a ColumNAR column can bisect an ordered column, match over
 * a dictionary's ordinals, and test a term once for every value naming it; a field framed as a blob per document has to
 * read every document and compare. Choosing between them once, here, keeps that decision out of the query methods on the
 * field type, which would otherwise each grow a branch per format.
 *
 * <p>Implementations are stateless, and {@link #forFormat} hands out one instance per format, so asking for them costs
 * nothing and a field type need not hold its own.
 */
public interface BinaryDocValuesQueries {

    /** The implementation for {@code format}, shared rather than built per call. */
    static BinaryDocValuesQueries forFormat(BinaryDocValuesFormat format) {
        return format == BinaryDocValuesFormat.COLUMNAR_PAYLOAD
            ? ColumnarBinaryDocValuesQueries.INSTANCE
            : ScanningBinaryDocValuesQueries.forFormat(format);
    }

    /** Documents holding exactly {@code term}. */
    Query term(String field, BytesRef term);

    /** Documents holding any of {@code terms}. */
    Query terms(String field, Collection<BytesRef> terms);

    /** Documents holding a value inside the bounds, either of which may be null for unbounded. */
    Query range(String field, @Nullable BytesRef lower, @Nullable BytesRef upper, boolean includeLower, boolean includeUpper);

    /** Documents holding a value that starts with {@code value}. */
    Query prefix(String field, String value, boolean caseInsensitive);

    /** Documents holding a value within {@code maxEdits} of {@code term}. */
    Query fuzzy(String field, String term, int maxEdits, int prefixLength, boolean transpositions);

    /** Documents holding {@code value}, compared without case. */
    Query caseInsensitiveTerm(String field, String value);

    /** Documents holding a value the wildcard pattern accepts. */
    Query wildcard(String field, String pattern, boolean caseInsensitive);

    /**
     * Documents holding a value {@code automaton} accepts, for a caller that has already decided what its pattern means.
     * {@code description} stands in for the automaton in equality, which is what lets two such queries share a cache entry.
     */
    Query automaton(String field, Automaton automaton, String description);

    /** Documents holding a value the regular expression accepts. */
    Query regexp(
        String field,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable CircuitBreaker breaker
    );
}
