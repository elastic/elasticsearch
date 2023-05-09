/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Collects results by running each filter against the searcher and doesn't
 * build any {@link LeafBucketCollector}s which is generally faster than
 * {@link Compatible} but doesn't support when there is a parent aggregator
 * or any child aggregators.
 */
public class FilterByFilterAggregator extends FiltersAggregator {
    /**
     * Builds {@link FilterByFilterAggregator} when the filters are valid and
     * it would be faster than a "native" aggregation implementation. The
     * interface is designed to allow easy construction of
     * {@link AdaptingAggregator}.
     */
    public abstract static class AdapterBuilder<T> {
        private final String name;
        private final List<QueryToFilterAdapter> filters = new ArrayList<>();
        private final boolean keyed;
        private final boolean keyedBucket;
        private final AggregationContext aggCtx;
        private final Aggregator parent;
        private final CardinalityUpperBound cardinality;
        private final Map<String, Object> metadata;
        private final Query rewrittenTopLevelQuery;
        private boolean valid = true;

        public AdapterBuilder(
            String name,
            boolean keyed,
            boolean keyedBucket,
            String otherBucketKey,
            AggregationContext aggCtx,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            this.name = name;
            this.keyed = keyed;
            this.keyedBucket = keyedBucket;
            this.aggCtx = aggCtx;
            this.parent = parent;
            this.cardinality = cardinality;
            this.metadata = metadata;
            this.rewrittenTopLevelQuery = aggCtx.searcher().rewrite(aggCtx.query());
            this.valid = parent == null && otherBucketKey == null && aggCtx.isInSortOrderExecutionRequired() == false;
        }

        /**
         * Subclasses should override this to adapt the
         * {@link FilterByFilterAggregator} into another sort of aggregator
         * if required.
         */
        protected abstract T adapt(CheckedFunction<AggregatorFactories, FilterByFilterAggregator, IOException> delegate) throws IOException;

        public final void add(String key, Query query) throws IOException {
            if (valid == false) {
                return;
            }
            if (query instanceof AbstractScriptFieldQuery) {
                /*
                 * We know that runtime fields aren't fast to query at all
                 * but we expect all other sorts of queries are at least as
                 * fast as the native aggregator.
                 */
                valid = false;
                return;
            }
            add(QueryToFilterAdapter.build(aggCtx.searcher(), key, query));
        }

        final void add(QueryToFilterAdapter filter) throws IOException {
            if (valid == false) {
                return;
            }
            QueryToFilterAdapter mergedFilter = filter.union(rewrittenTopLevelQuery);
            if (mergedFilter.isInefficientUnion()) {
                /*
                 * For now any complex union kicks us out of filter by filter
                 * mode. Its possible that this de-optimizes many "filters"
                 * aggregations but likely correct when "range", "date_histogram",
                 * or "terms" are converted to this agg. We investigated a sort
                 * of "combined" iteration mechanism and its complex *and* slower
                 * than the native implementations of the aggs above.
                 */
                valid = false;
                return;
            }
            if (filters.size() == 1) {
                /*
                 * When we add the second filter we check if there are any _doc_count
                 * fields and bail out of filter-by filter mode if there are. _doc_count
                 * fields are expensive to decode and the overhead of iterating per
                 * filter causes us to decode doc counts over and over again.
                 */
                if (aggCtx.hasDocCountField()) {
                    valid = false;
                    return;
                }
            }
            filters.add(mergedFilter);
        }

        /**
         * Build the the adapter or {@code null} if the this isn't a valid rewrite.
         */
        public final T build() throws IOException {
            if (false == valid) {
                return null;
            }
            class AdapterBuild implements CheckedFunction<AggregatorFactories, FilterByFilterAggregator, IOException> {
                private FilterByFilterAggregator agg;

                @Override
                public FilterByFilterAggregator apply(AggregatorFactories subAggregators) throws IOException {
                    agg = new FilterByFilterAggregator(
                        name,
                        subAggregators,
                        filters,
                        keyed,
                        keyedBucket,
                        aggCtx,
                        parent,
                        cardinality,
                        metadata
                    );
                    return agg;
                }
            }
            AdapterBuild adapterBuild = new AdapterBuild();
            T result = adapt(adapterBuild);
            if (adapterBuild.agg.scoreMode().needsScores()) {
                /*
                 * Filter by filter won't produce the correct results if the
                 * sub-aggregators need scores because we're not careful with how
                 * we merge filters. Right now we have to build the whole
                 * aggregation in order to know if it'll need scores or not.
                 * This means we'll build the *sub-aggs* too. Oh well.
                 */
                return null;
            }
            return result;
        }
    }

    /**
     * Count of segments with "live" docs. This is both deleted docs and
     * docs covered by field level security.
     */
    private int segmentsWithDeletedDocs;
    /**
     * Count of segments with documents have consult the {@code doc_count}
     * field.
     */
    private int segmentsWithDocCountField;
    /**
     * Count of segments this aggregator performed a document by document
     * collection for. We have to collect when there are sub-aggregations
     * and it disables some optimizations we can make while just counting.
     */
    private int segmentsCollected;
    /**
     * Count of segments this aggregator counted. We can count when there
     * aren't any sub-aggregators and we have some counting optimizations
     * that don't apply to document by document collections.
     * <p>
     * But the "fallback" for counting when we don't have a fancy optimization
     * is to perform document by document collection and increment a counter
     * on each document. This fallback does not increment the
     * {@link #segmentsCollected} counter and <strong>does</strong> increment
     * the {@link #segmentsCounted} counter because those counters are to
     * signal which operation we were allowed to perform. The filters
     * themselves will have debugging counters measuring if they could
     * perform the count from metadata or had to fall back.
     */
    private int segmentsCounted;

    /**
     * Build the aggregation. Private to force callers to go through the
     * {@link AdapterBuilder} which centralizes the logic to decide if this
     * aggregator would be faster than the native implementation.
     */
    private FilterByFilterAggregator(
        String name,
        AggregatorFactories factories,
        List<QueryToFilterAdapter> filters,
        boolean keyed,
        boolean keyedBucket,
        AggregationContext aggCtx,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, filters, keyed, keyedBucket, null, aggCtx, parent, cardinality, metadata);
    }

    /**
     * Instead of returning a {@link LeafBucketCollector} we do the
     * collection ourselves by running the filters directly. This is safe
     * because we only use this aggregator if there isn't a {@code parent}
     * which would change how we collect buckets and because we take the
     * top level query into account when building the filters.
     */
    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        assert scoreMode().needsScores() == false;
        if (filters().size() == 0) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        Bits live = aggCtx.getLeafReaderContext().reader().getLiveDocs();
        if (live != null) {
            segmentsWithDeletedDocs++;
        }
        if (false == docCountProvider.alwaysOne()) {
            segmentsWithDocCountField++;
        }
        if (subAggregators.length == 0) {
            // TOOD we'd be better off if we could do sub.isNoop() or something.
            /*
             * Without sub.isNoop we always end up in the `collectXXX` modes even if
             * the sub-aggregators opt out of traditional collection.
             */
            segmentsCounted++;
            collectCount(aggCtx.getLeafReaderContext(), live);
        } else {
            segmentsCollected++;
            collectSubs(aggCtx, live, sub);
        }
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    /**
     * Gather a count of the number of documents that match each filter
     * without sending any documents to a sub-aggregator. This yields
     * the correct response when there aren't any sub-aggregators or they
     * all opt out of needing any sort of collection.
     */
    private void collectCount(LeafReaderContext ctx, Bits live) throws IOException {
        Counter counter = new Counter(docCountProvider);
        for (int filterOrd = 0; filterOrd < filters().size(); filterOrd++) {
            incrementBucketDocCount(filterOrd, filters().get(filterOrd).count(ctx, counter, live));
        }
    }

    /**
     * Collect all documents that match all filters and send them to
     * the sub-aggregators. This method is only required when there are
     * sub-aggregators that haven't opted out of being collected.
     * <p>
     * This collects each filter one at a time, resetting the
     * sub-aggregators between each filter as though they were hitting
     * a fresh segment.
     * <p>
     * It's <strong>very</strong> tempting to try and collect the
     * filters into blocks of matches and then reply the whole block
     * into ascending order without the resetting. That'd probably
     * work better if the disk was very, very slow and we didn't have
     * any kind of disk caching. But with disk caching its about twice
     * as fast to collect each filter one by one like this. And it uses
     * less memory because there isn't a need to buffer a block of matches.
     * And its a hell of a lot less code.
     */
    private void collectSubs(AggregationExecutionContext aggCtx, Bits live, LeafBucketCollector sub) throws IOException {
        class MatchCollector implements LeafCollector {
            LeafBucketCollector subCollector = sub;
            int filterOrd;

            @Override
            public void collect(int docId) throws IOException {
                collectBucket(subCollector, docId, filterOrd);
            }

            @Override
            public void setScorer(Scorable scorer) throws IOException {}
        }
        MatchCollector collector = new MatchCollector();
        filters().get(0).collect(aggCtx.getLeafReaderContext(), collector, live);
        for (int filterOrd = 1; filterOrd < filters().size(); filterOrd++) {
            collector.subCollector = collectableSubAggregators.getLeafCollector(aggCtx);
            collector.filterOrd = filterOrd;
            filters().get(filterOrd).collect(aggCtx.getLeafReaderContext(), collector, live);
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("segments_counted", segmentsCounted);
        add.accept("segments_collected", segmentsCollected);
        add.accept("segments_with_deleted_docs", segmentsWithDeletedDocs);
        add.accept("segments_with_doc_count_field", segmentsWithDocCountField);
    }

}
