/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Wraps the root {@link IntervalsSource} of an intervals query so that a multi-term rule (prefix, wildcard, regexp,
 * fuzzy, range) expanding to more terms than its limit is reported as a client error.
 * <p>
 * Lucene enforces the expansion limit while it iterates the terms of a segment, when the scorer or the matches are
 * built, and reports it as an {@link IllegalStateException}. The request is what exceeds the limit, so this source
 * rethrows that exception as an {@link IllegalArgumentException}, which makes the search fail with a 400. The message
 * names the field, keeps Lucene's message and asks for a more selective pattern. Every other method delegates to the
 * wrapped source, so equality, the query string and visitors behave as they do for the wrapped source.
 */
// TODO: remove once Lucene throws IndexSearcher.TooManyClauses for intervals over-expansion (apache/lucene#16899)
// and TooManyClauses is mapped to a 400.
final class ExpansionLimitIntervalsSource extends IntervalsSource {

    /**
     * Text of the message Lucene's {@code MultiTermIntervalsSource} uses when a rule expands to too many terms.
     */
    static final String EXPANSION_LIMIT_MESSAGE = "expanded to too many terms";

    private final IntervalsSource in;

    ExpansionLimitIntervalsSource(IntervalsSource in) {
        this.in = Objects.requireNonNull(in);
    }

    @Override
    public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
        try {
            return in.intervals(field, ctx);
        } catch (IllegalStateException e) {
            throw translate(field, e);
        }
    }

    @Override
    public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
        try {
            return in.matches(field, ctx, doc);
        } catch (IllegalStateException e) {
            throw translate(field, e);
        }
    }

    private static RuntimeException translate(String field, IllegalStateException e) {
        if (isExpansionLimit(e)) {
            return new IllegalArgumentException(
                "[intervals] rule on field [" + field + "] " + e.getMessage() + "; use a more selective pattern",
                e
            );
        }
        return e;
    }

    /**
     * Whether {@code e} is Lucene's report that a multi-term rule expanded to more terms than its limit.
     */
    static boolean isExpansionLimit(IllegalStateException e) {
        String message = e.getMessage();
        return message != null && message.contains(EXPANSION_LIMIT_MESSAGE);
    }

    @Override
    public void visit(String field, QueryVisitor visitor) {
        in.visit(field, visitor);
    }

    @Override
    public int minExtent() {
        return in.minExtent();
    }

    /**
     * This source only ever wraps the root of an intervals query, so it has no parent that would pull up its disjunctions.
     */
    @Override
    public Collection<IntervalsSource> pullUpDisjunctions() {
        return List.of(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return in.equals(((ExpansionLimitIntervalsSource) o).in);
    }

    @Override
    public int hashCode() {
        return in.hashCode();
    }

    @Override
    public String toString() {
        return in.toString();
    }
}
