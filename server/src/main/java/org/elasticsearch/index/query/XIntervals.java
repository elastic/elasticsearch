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
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Copy of {@link Intervals} that exposes versions of {@link Intervals#ordered} and {@link Intervals#unordered}
 * that preserve their inner gaps.
 * NOTE: Remove this hack when a version of Lucene with https://github.com/apache/lucene/pull/13819 is used (10.1.0).
 */
public final class XIntervals {

    /**
     * Create an ordered {@link IntervalsSource}
     *
     * <p>Returns intervals in which the subsources all appear in the given order
     *
     * @param subSources an ordered set of {@link IntervalsSource} objects
     */
    public static IntervalsSource ordered(IntervalsSource... subSources) {
        return new DelegateIntervalsSource(Intervals.ordered(subSources));
    }

    /**
     * Create an ordered {@link IntervalsSource}
     *
     * <p>Returns intervals in which the subsources all appear in the given order
     *
     * @param subSources an ordered set of {@link IntervalsSource} objects
     */
    public static IntervalsSource unordered(IntervalsSource... subSources) {
        return new DelegateIntervalsSource(Intervals.unordered(subSources));
    }

    /**
     * Wraps a source to avoid aggressive flattening of the ordered and unordered sources.
     * The flattening modifies the final gap and is removed in the latest unreleased version of Lucene (10.1).
     */
    private static class DelegateIntervalsSource extends IntervalsSource {
        private final IntervalsSource delegate;

        private DelegateIntervalsSource(IntervalsSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
            return delegate.intervals(field, ctx);
        }

        @Override
        public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
            return delegate.matches(field, ctx, doc);
        }

        @Override
        public void visit(String field, QueryVisitor visitor) {
            delegate.visit(field, visitor);
        }

        @Override
        public int minExtent() {
            return delegate.minExtent();
        }

        @Override
        public Collection<IntervalsSource> pullUpDisjunctions() {
            return delegate.pullUpDisjunctions();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DelegateIntervalsSource that = (DelegateIntervalsSource) o;
            return Objects.equals(delegate, that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
