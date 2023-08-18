/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper of {@link IntervalsSource} for the case when positions are not indexed.
 */
public final class SourceIntervalsSource extends IntervalsSource {

    private final IntervalsSource in;
    private final Query approximation;
    private final IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider;
    private final Analyzer indexAnalyzer;

    public SourceIntervalsSource(
        IntervalsSource in,
        Query approximation,
        IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider,
        Analyzer indexAnalyzer
    ) {
        this.in = Objects.requireNonNull(in);
        this.approximation = Objects.requireNonNull(approximation);
        this.valueFetcherProvider = Objects.requireNonNull(valueFetcherProvider);
        this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
    }

    public IntervalsSource getIntervalsSource() {
        return in;
    }

    private LeafReaderContext createSingleDocLeafReaderContext(String field, List<Object> values) {
        MemoryIndex index = new MemoryIndex();
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            index.addField(field, value.toString(), indexAnalyzer);
        }
        index.freeze();
        return index.createSearcher().getIndexReader().leaves().get(0);
    }

    @Override
    public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(ctx.reader());
        final Weight weight = searcher.createWeight(searcher.rewrite(approximation), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final Scorer scorer = weight.scorer(ctx.reader().getContext());
        if (scorer == null) {
            return null;
        }
        final DocIdSetIterator approximationIter = scorer.iterator();

        final CheckedIntFunction<List<Object>, IOException> valueFetcher = valueFetcherProvider.apply(ctx);
        return new IntervalIterator() {

            private IntervalIterator in;

            @Override
            public int docID() {
                return approximationIter.docID();
            }

            @Override
            public long cost() {
                return approximationIter.cost();
            }

            @Override
            public int nextDoc() throws IOException {
                return doNext(approximationIter.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return doNext(approximationIter.advance(target));
            }

            private int doNext(int doc) throws IOException {
                while (doc != NO_MORE_DOCS && setIterator(doc) == false) {
                    doc = approximationIter.nextDoc();
                }
                return doc;
            }

            private boolean setIterator(int doc) throws IOException {
                final List<Object> values = valueFetcher.apply(doc);
                final LeafReaderContext singleDocContext = createSingleDocLeafReaderContext(field, values);
                in = SourceIntervalsSource.this.in.intervals(field, singleDocContext);
                final boolean isSet = in != null && in.nextDoc() != NO_MORE_DOCS;
                assert isSet == false || in.docID() == 0;
                return isSet;
            }

            @Override
            public int start() {
                return in.start();
            }

            @Override
            public int end() {
                return in.end();
            }

            @Override
            public int gaps() {
                return in.gaps();
            }

            @Override
            public int nextInterval() throws IOException {
                return in.nextInterval();
            }

            @Override
            public float matchCost() {
                // a high number since we need to parse the _source
                return 10_000;
            }

        };
    }

    @Override
    public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
        final CheckedIntFunction<List<Object>, IOException> valueFetcher = valueFetcherProvider.apply(ctx);
        final List<Object> values = valueFetcher.apply(doc);
        final LeafReaderContext singleDocContext = createSingleDocLeafReaderContext(field, values);
        return in.matches(field, singleDocContext, 0);
    }

    @Override
    public void visit(String field, QueryVisitor visitor) {
        in.visit(field, visitor);
    }

    @Override
    public int minExtent() {
        return in.minExtent();
    }

    @Override
    public Collection<IntervalsSource> pullUpDisjunctions() {
        return Collections.singleton(this);
    }

    @Override
    public int hashCode() {
        // Not using matchesProvider and valueFetcherProvider, which don't identify this source but are only used to avoid scanning linearly
        // through all documents
        return Objects.hash(in, indexAnalyzer);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SourceIntervalsSource that = (SourceIntervalsSource) other;
        // Not using matchesProvider and valueFetcherProvider, which don't identify this source but are only used to avoid scanning linearly
        // through all documents
        return in.equals(that.in) && indexAnalyzer.equals(that.indexAnalyzer);
    }

    @Override
    public String toString() {
        return in.toString();
    }

}
