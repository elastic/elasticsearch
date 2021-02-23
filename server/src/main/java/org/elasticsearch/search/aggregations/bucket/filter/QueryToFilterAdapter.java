/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;

/**
 * Adapts a Lucene {@link Query} to the behaviors used be the
 * {@link FiltersAggregator}. In general we try to delegate to {@linkplain Query}
 * when we don't have a special optimization.
 */
public abstract class QueryToFilterAdapter {
    /**
     * Build a filter against the provided searcher.
     * <p>
     * Note: This method rewrites the query against the {@link IndexSearcher}
     */
    public static QueryToFilterAdapter build(IndexSearcher searcher, String key, Query query) throws IOException {
        query = searcher.rewrite(query);
        if (query instanceof TermQuery) {
            return new TermQueryToFilterAdapter(searcher, key, (TermQuery) query);
        }
        if (query instanceof MatchAllDocsQuery) {
            return new MatchAllQueryToFilterAdapter(searcher, key);
        }
        if (query instanceof MatchNoDocsQuery) {
            return new MatchNoneQueryToFilterAdapter(searcher, key);
        }
        return new DefaultQueryToFilterAdapter(searcher, key, query);
    }

    private final IndexSearcher searcher;
    private final String key;

    private QueryToFilterAdapter(IndexSearcher searcher, String key) {
        this.searcher = searcher;
        this.key = key;
    }

    public final String key() {
        return key;
    }

    protected final IndexSearcher searcher() {
        return searcher;
    }

    /**
     * Is it safe to use index metadata like
     * {@link IndexReader#docFreq} or {@link IndexReader#maxDoc} to count the
     * number of matching documents.
     */
    protected final boolean countCanUseMetadata(FiltersAggregator.Counter counter, Bits live) {
        if (live != null) {
            /*
             * We can only use metadata if all of the documents in the reader
             * are visible. This is done by returning a null `live` bits. The
             * name `live` is traditional because most of the time a non-null
             * `live` bits means that there are deleted documents. But `live`
             * might also be non-null if document level security is enabled.
             */
            return false;
        }
        /*
         * We can only use metadata if we're not using the special docCount
         * field. Otherwise we wouldn't know how many documents each lucene
         * document represents.
         */
        return counter.docCount.alwaysOne();
    }

    /**
     * Make a filter that matches this filter and the provided query.
     * <p>
     * Note: This method rewrites the query against the {@link IndexSearcher}.
     */
    abstract QueryToFilterAdapter union(Query extraQuery) throws IOException;

    abstract IntPredicate matchingDocIds(LeafReaderContext ctx) throws IOException;

    abstract long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException;

    /**
     * Estimate the cost of calling {@code #count} on this leaf.
     */
    abstract long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException;

    /**
     * Collect profiling information for this filter.
     */
    void collectDebugInfo(BiConsumer<String, Object> add) {}

    /**
     * Special case when the filter can't match anything.
     */
    private static class MatchNoneQueryToFilterAdapter extends QueryToFilterAdapter {
        private MatchNoneQueryToFilterAdapter(IndexSearcher searcher, String key) {
            super(searcher, key);
        }

        @Override
        QueryToFilterAdapter union(Query extraQuery) throws IOException {
            return this;
        }

        @Override
        IntPredicate matchingDocIds(LeafReaderContext ctx) throws IOException {
            return l -> false;
        }

        @Override
        long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
            return 0;
        }

        @Override
        long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException {
            return 0;
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("type", "match_none");
        }
    }

    /**
     * Filter that matches every document.
     */
    private static class MatchAllQueryToFilterAdapter extends QueryToFilterAdapter {
        private int resultsFromMetadata;

        private MatchAllQueryToFilterAdapter(IndexSearcher searcher, String key) {
            super(searcher, key);
        }

        @Override
        QueryToFilterAdapter union(Query extraQuery) throws IOException {
            return QueryToFilterAdapter.build(searcher(), key(), extraQuery);
        }

        @Override
        IntPredicate matchingDocIds(LeafReaderContext ctx) throws IOException {
            return l -> true;
        }

        @Override
        long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
            if (countCanUseMetadata(counter, live)) {
                resultsFromMetadata++;
                return ctx.reader().maxDoc();
            }
            new MatchAllDocsQuery().createWeight(searcher(), ScoreMode.COMPLETE_NO_SCORES, 1.0f).bulkScorer(ctx).score(counter, live);
            return counter.readAndReset(ctx);
        }

        @Override
        long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException {
            return canUseMetadata.get() ? 0 : ctx.reader().maxDoc();
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("type", "match_all");
            add.accept("results_from_metadata", resultsFromMetadata);
        }
    }

    /**
     * Abstract superclass of filters that delegates everything to the query.
     */
    private abstract static class CommonQueryToFilterAdapter<Q extends Query> extends QueryToFilterAdapter {
        private final Q query;
        private Weight weight;
        private BulkScorer[] bulkScorers;
        private int scorersPreparedWhileEstimatingCost;

        private CommonQueryToFilterAdapter(IndexSearcher searcher, String key, Q query) {
            super(searcher, key);
            this.query = query;
        }

        /**
         * The query we're adapting.
         * <p>
         * Subclasses should use this to fetch the query when making query
         * specific optimizations.
         */
        protected Q query() {
            return query;
        }

        @Override
        public QueryToFilterAdapter union(Query extraQuery) throws IOException {
            extraQuery = searcher().rewrite(extraQuery);
            if (extraQuery instanceof MatchAllDocsQuery) {
                return this;
            }
            Query unwrappedQuery = unwrap(query);
            Query unwrappedExtraQuery = unwrap(extraQuery);
            if (unwrappedQuery instanceof PointRangeQuery && unwrappedExtraQuery instanceof PointRangeQuery) {
                Query merged = MergedPointRangeQuery.merge((PointRangeQuery) unwrappedQuery, (PointRangeQuery) unwrappedExtraQuery);
                if (merged != null) {
                    // Should we rewrap here?
                    return new DefaultQueryToFilterAdapter(searcher(), key(), merged);
                }
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, BooleanClause.Occur.MUST);
            builder.add(extraQuery, BooleanClause.Occur.MUST);
            return new DefaultQueryToFilterAdapter(searcher(), key(), builder.build());
        }

        private static Query unwrap(Query query) {
            while (true) {
                if (query instanceof ConstantScoreQuery) {
                    query = ((ConstantScoreQuery) query).getQuery();
                    continue;
                }
                if (query instanceof IndexSortSortedNumericDocValuesRangeQuery) {
                    query = ((IndexSortSortedNumericDocValuesRangeQuery) query).getFallbackQuery();
                    continue;
                }
                if (query instanceof IndexOrDocValuesQuery) {
                    query = ((IndexOrDocValuesQuery) query).getIndexQuery();
                    continue;
                }
                return query;
            }
        }

        @Override
        @SuppressWarnings("resource")  // Closing the reader is someone else's problem
        public IntPredicate matchingDocIds(LeafReaderContext ctx) throws IOException {
            return Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), weight().scorerSupplier(ctx))::get;
        }

        @Override
        long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
            BulkScorer scorer = bulkScorer(ctx, () -> {});
            if (scorer == null) {
                // No hits in this segment.
                return 0;
            }
            scorer.score(counter, live);
            return counter.readAndReset(ctx);
        }

        @Override
        long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException {
            BulkScorer scorer = bulkScorer(ctx, () -> scorersPreparedWhileEstimatingCost++);
            if (scorer == null) {
                // There aren't any matches for this filter in this leaf
                return 0;
            }
            return scorer.cost();
        }

        private BulkScorer bulkScorer(LeafReaderContext ctx, Runnable onPrepare) throws IOException {
            if (bulkScorers == null) {
                bulkScorers = new BulkScorer[searcher().getIndexReader().leaves().size()];
            }
            if (bulkScorers[ctx.ord] == null) {
                onPrepare.run();
                return bulkScorers[ctx.ord] = weight().bulkScorer(ctx);
            }
            return bulkScorers[ctx.ord];
        }

        private Weight weight() throws IOException {
            if (weight == null) {
                weight = searcher().createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            }
            return weight;
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("query", query.toString());
            add.accept("scorers_prepared_while_estimating_cost", scorersPreparedWhileEstimatingCost);
        }
    }

    private static class DefaultQueryToFilterAdapter extends CommonQueryToFilterAdapter<Query> {
        private DefaultQueryToFilterAdapter(IndexSearcher searcher, String key, Query query) {
            super(searcher, key, query);
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("type", "query");
        }
    }

    private static class TermQueryToFilterAdapter extends CommonQueryToFilterAdapter<TermQuery> {
        private int resultsFromMetadata;

        private TermQueryToFilterAdapter(IndexSearcher searcher, String key, TermQuery query) {
            super(searcher, key, query);
        }

        @Override
        long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
            if (countCanUseMetadata(counter, live)) {
                resultsFromMetadata++;
                return ctx.reader().docFreq(query().getTerm());
            }
            return super.count(ctx, counter, live);
        }

        @Override
        long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException {
            if (canUseMetadata.get()) {
                return 0;
            }
            return super.estimateCountCost(ctx, canUseMetadata);
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("type", "term");
            add.accept("results_from_metadata", resultsFromMetadata);
        }
    }
}
