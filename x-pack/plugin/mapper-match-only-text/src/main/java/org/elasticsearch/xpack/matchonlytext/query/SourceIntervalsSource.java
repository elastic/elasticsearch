package org.elasticsearch.xpack.matchonlytext.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedIntFunction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A wrapper of {@link IntervalsSource} for the case when positions are not indexed.
 */
public final class SourceIntervalsSource extends IntervalsSource {

    private final IntervalsSource in;
    private final Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider;
    private final Analyzer indexAnalyzer;

    public SourceIntervalsSource(IntervalsSource in,
            Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider,
            Analyzer indexAnalyzer) {
        this.in = Objects.requireNonNull(in);
        this.valueFetcherProvider = Objects.requireNonNull(valueFetcherProvider);
        this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
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
        // TODO: How can we extract a better approximation from this IntervalsSource?
        final DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
        final CheckedIntFunction<List<Object>, IOException> valueFetcher = valueFetcherProvider.apply(ctx);
        return new IntervalIterator() {

            private IntervalIterator in;

            @Override
            public int docID() {
                return approximation.docID();
            }

            @Override
            public long cost() {
                return approximation.cost();
            }

            @Override
            public int nextDoc() throws IOException {
                return doNext(approximation.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return doNext(approximation.advance(target));
            }

            private int doNext(int doc) throws IOException {
                while (doc != NO_MORE_DOCS && setIterator(doc) == false) {
                    doc = approximation.nextDoc();
                }
                return doc;
            }

            private boolean setIterator(int doc) {
                try {
                    final List<Object> values = valueFetcher.apply(doc);
                    final LeafReaderContext singleDocContext = createSingleDocLeafReaderContext(field, values);
                    in = SourceIntervalsSource.this.in.intervals(field, singleDocContext);
                    return in.nextDoc() != NO_MORE_DOCS;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
        // Not using matchesProvider and valueFetcherProvider, which don't identify this source but are only used to avoid scanning linearly through all documents
        return Objects.hash(in, indexAnalyzer);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SourceIntervalsSource that = (SourceIntervalsSource) other;
        // Not using matchesProvider and valueFetcherProvider, which don't identify this source but are only used to avoid scanning linearly through all documents
        return in.equals(that.in) && indexAnalyzer.equals(that.indexAnalyzer);
    }

    @Override
    public String toString() {
        return in.toString();
    }

}
