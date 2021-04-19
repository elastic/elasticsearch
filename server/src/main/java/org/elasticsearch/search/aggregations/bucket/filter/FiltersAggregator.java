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
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

/**
 * Aggregator for {@code filters}. There are two known subclasses,
 * {@link FilterByFilter} which is fast but only works in some cases and
 * {@link Compatible} which works in all cases.
 * {@link FiltersAggregator#build} will build the fastest version that
 * works with the configuration.
 */
public abstract class FiltersAggregator extends BucketsAggregator {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");
    public static final ParseField OTHER_BUCKET_FIELD = new ParseField("other_bucket");
    public static final ParseField OTHER_BUCKET_KEY_FIELD = new ParseField("other_bucket_key");

    public static class KeyedFilter implements Writeable, ToXContentFragment {
        private final String key;
        private final QueryBuilder filter;

        public KeyedFilter(String key, QueryBuilder filter) {
            if (key == null) {
                throw new IllegalArgumentException("[key] must not be null");
            }
            if (filter == null) {
                throw new IllegalArgumentException("[filter] must not be null");
            }
            this.key = key;
            this.filter = filter;
        }

        /**
         * Read from a stream.
         */
        public KeyedFilter(StreamInput in) throws IOException {
            key = in.readString();
            filter = in.readNamedWriteable(QueryBuilder.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeNamedWriteable(filter);
        }

        public String key() {
            return key;
        }

        public QueryBuilder filter() {
            return filter;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(key, filter);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, filter);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            KeyedFilter other = (KeyedFilter) obj;
            return Objects.equals(key, other.key)
                    && Objects.equals(filter, other.filter);
        }
    }

    /**
     * Build an {@link Aggregator} for a {@code filters} aggregation. If there
     * isn't a parent, there aren't children, and we don't collect "other"
     * buckets then this will a faster {@link FilterByFilter} aggregator.
     * Otherwise it'll fall back to a slower aggregator that is
     * {@link Compatible} with parent, children, and "other" buckets.
     */
    public static FiltersAggregator build(
        String name,
        AggregatorFactories factories,
        List<QueryToFilterAdapter<?>> filters,
        boolean keyed,
        String otherBucketKey,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (canUseFilterByFilter(parent, otherBucketKey)) {
            FilterByFilter filterByFilter = buildFilterByFilter(
                name,
                factories,
                filters,
                keyed,
                otherBucketKey,
                context,
                parent,
                cardinality,
                metadata
            );
            if (false == filterByFilter.scoreMode().needsScores()) {
                /*
                 * Filter by filter won't produce the correct results if the
                 * sub-aggregators need scores because we're not careful with how
                 * we merge filters. Right now we have to build the whole
                 * aggregation in order to know if it'll need scores or not.
                 */
                // TODO make filter by filter produce the correct result or skip this in canUseFilterbyFilter
                return filterByFilter;
            }
        }
        return new FiltersAggregator.Compatible(
            name,
            factories,
            filters,
            keyed,
            otherBucketKey,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    /**
     * Can this aggregation be executed using the {@link FilterByFilter}? That
     * aggregator is much faster than the fallback {@link Compatible} aggregator.
     */
    public static boolean canUseFilterByFilter(Aggregator parent, String otherBucketKey) {
        return parent == null && otherBucketKey == null;
    }

    /**
     * Build an {@link Aggregator} for a {@code filters} aggregation if we
     * can collect {@link FilterByFilter}, otherwise return {@code null}. We can
     * collect filter by filter if there isn't a parent, there aren't children,
     * and we don't collect "other" buckets. Collecting {@link FilterByFilter}
     * is generally going to be much faster than the {@link Compatible} aggregator.
     * <p>
     * <strong>Important:</strong> This doesn't properly handle sub-aggregators
     * that need scores so callers must check {@code #scoreMode()} and not use
     * this collector if it need scores.
     */
    public static FilterByFilter buildFilterByFilter(
        String name,
        AggregatorFactories factories,
        List<QueryToFilterAdapter<?>> filters,
        boolean keyed,
        String otherBucketKey,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (false == canUseFilterByFilter(parent, otherBucketKey)) {
            throw new IllegalStateException("Can't execute filter-by-filter");
        }
        List<QueryToFilterAdapter<?>> filtersWithTopLevel = new ArrayList<>(filters.size());
        for (QueryToFilterAdapter<?> f : filters) {
            filtersWithTopLevel.add(f.union(context.query()));
        }
        return new FiltersAggregator.FilterByFilter(
            name,
            factories,
            filtersWithTopLevel,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private final List<QueryToFilterAdapter<?>> filters;
    private final boolean keyed;
    protected final String otherBucketKey;

    private FiltersAggregator(String name, AggregatorFactories factories, List<QueryToFilterAdapter<?>> filters, boolean keyed,
            String otherBucketKey, AggregationContext context, Aggregator parent, CardinalityUpperBound cardinality,
            Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, cardinality.multiply(filters.size() + (otherBucketKey == null ? 0 : 1)), metadata);
        this.filters = List.copyOf(filters);
        this.keyed = keyed;
        this.otherBucketKey = otherBucketKey;
    }

    List<QueryToFilterAdapter<?>> filters() {
        return filters;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(owningBucketOrds, filters.size() + (otherBucketKey == null ? 0 : 1),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                if (offsetInOwningOrd < filters.size()) {
                    return new InternalFilters.InternalBucket(filters.get(offsetInOwningOrd).key().toString(), docCount,
                            subAggregationResults, keyed);
                }
                return new InternalFilters.InternalBucket(otherBucketKey, docCount, subAggregationResults, keyed);
            }, buckets -> new InternalFilters(name, buckets, keyed, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(filters.size() + otherBucketKey == null ? 0 : 1);
        for (QueryToFilterAdapter<?> filter : filters) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(filter.key().toString(), 0, subAggs, keyed);
            buckets.add(bucket);
        }

        if (otherBucketKey != null) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(otherBucketKey, 0, subAggs, keyed);
            buckets.add(bucket);
        }

        return new InternalFilters(name, buckets, keyed, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        List<Map<String, Object>> filtersDebug = new ArrayList<>(filters.size());
        for (QueryToFilterAdapter<?> filter : filters) {
            Map<String, Object> debug = new HashMap<>();
            filter.collectDebugInfo(debug::put);
            filtersDebug.add(debug);
        }
        add.accept("filters", filtersDebug);
    }

    /**
     * Collects results by running each filter against the searcher and doesn't
     * build any {@link LeafBucketCollector}s which is generally faster than
     * {@link Compatible} but doesn't support when there is a parent aggregator
     * or any child aggregators.
     */
    public static class FilterByFilter extends FiltersAggregator {
        private final boolean profiling;
        private long estimatedCost = -1;
        /**
         * The maximum allowed estimated cost. Defaults to {@code -1} meaning no
         * max but can be set. Used for emitting debug info.
         */
        private long maxCost = -1;
        private long estimateCostTime;
        private int segmentsWithDeletedDocs;
        /**
         * Count of segments with documents have consult the {@code doc_count}
         * field.
         */
        private int segmentsWithDocCountField;
        private int segmentsCollected;
        private int segmentsCounted;

        private FilterByFilter(
            String name,
            AggregatorFactories factories,
            List<QueryToFilterAdapter<?>> filters,
            boolean keyed,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, filters, keyed, null, context, parent, cardinality, metadata);
            this.profiling = context.profiling();
        }

        /**
         * Estimate the number of documents that this aggregation must visit. We'll
         * stop counting once we've passed {@code maxEstimatedCost} if we aren't profiling.
         */
        @SuppressWarnings("resource") // We're not in change of anything Closeable
        public long estimateCost(long maxCost) throws IOException {
            assert scoreMode().needsScores() == false;
            // TODO if we have children we should use a different cost estimate
            this.maxCost = maxCost;
            if (estimatedCost != -1) {
                return estimatedCost;
            }
            long start = profiling ? System.nanoTime() : 0;
            estimatedCost = 0;
            for (LeafReaderContext ctx : searcher().getIndexReader().leaves()) {
                CheckedSupplier<Boolean, IOException> canUseMetadata = canUseMetadata(ctx);
                for (QueryToFilterAdapter<?> filter : filters()) {
                    estimatedCost += subAggregators().length > 0
                        ? filter.estimateCollectCost(ctx)
                        : filter.estimateCountCost(ctx, canUseMetadata);
                    if (estimatedCost < 0) {
                        // We've overflowed so we cap out and stop counting.
                        estimatedCost = Long.MAX_VALUE;
                        if (profiling && estimateCostTime == 0) {
                            estimateCostTime = System.nanoTime() - start;
                        }
                        return estimatedCost;
                    }
                    if (estimatedCost > maxCost) {
                        if (profiling) {
                            /*
                             * If we're profiling we stop the timer the first
                             * time we pass the limit but we keep counting so
                             * we get an accurate estimate.
                             */
                            if (estimateCostTime == 0) {
                                estimateCostTime = System.nanoTime() - start;
                            }
                        } else {
                            // We're past the limit and not profiling. No use counting further.
                            return estimatedCost;
                        }
                    }
                }
            }
            if (profiling && estimateCostTime == 0) {
                estimateCostTime = System.nanoTime() - start;
            }
            return estimatedCost;
        }

        /**
         * Instead of returning a {@link LeafBucketCollector} we do the
         * collection ourselves by running the filters directly. This is safe
         * because we only use this aggregator if there isn't a {@code parent}
         * which would change how we collect buckets and because we take the
         * top level query into account when building the filters.
         */
        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            assert scoreMode().needsScores() == false;
            if (filters().size() == 0) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
            Bits live = ctx.reader().getLiveDocs();
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
                collectCount(ctx, live);
            } else {
                segmentsCollected++;
                collectSubs(ctx, live, sub);
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
        private void collectSubs(LeafReaderContext ctx, Bits live, LeafBucketCollector sub) throws IOException {
            class MatchCollector implements LeafCollector {
                LeafBucketCollector subCollector = sub;
                int filterOrd;

                @Override
                public void collect(int docId) throws IOException {
                    collectBucket(subCollector, docId, filterOrd);
                }

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                }
            }
            MatchCollector collector = new MatchCollector();
            filters().get(0).collect(ctx, collector, live);
            for (int filterOrd = 1; filterOrd < filters().size(); filterOrd++) {
                collector.subCollector = collectableSubAggregators.getLeafCollector(ctx);
                collector.filterOrd = filterOrd;
                filters().get(filterOrd).collect(ctx, collector, live);
            }
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("segments_counted", segmentsCounted);
            add.accept("segments_collected", segmentsCollected);
            add.accept("segments_with_deleted_docs", segmentsWithDeletedDocs);
            add.accept("segments_with_doc_count_field", segmentsWithDocCountField);
            if (estimatedCost != -1) {
                // -1 means we didn't estimate it.
                add.accept("estimated_cost", estimatedCost);
                add.accept("max_cost", maxCost);
                add.accept("estimate_cost_time", estimateCostTime);
            }
        }

        CheckedSupplier<Boolean, IOException> canUseMetadata(LeafReaderContext ctx) {
            return new CheckedSupplier<Boolean, IOException>() {
                Boolean canUse;

                @Override
                public Boolean get() throws IOException {
                    if (canUse == null) {
                        canUse = canUse();
                    }
                    return canUse;
                }

                private boolean canUse() throws IOException {
                    if (ctx.reader().getLiveDocs() != null) {
                        return false;
                    }
                    docCountProvider.setLeafReaderContext(ctx);
                    return docCountProvider.alwaysOne();
                }
            };
        }
    }

    /**
     * Collects results by building a {@link LongPredicate} per filter and testing if
     * each doc sent to its {@link LeafBucketCollector} is in each filter
     * which is generally slower than {@link FilterByFilter} but is compatible
     * with parent and child aggregations.
     */
    private static class Compatible extends FiltersAggregator {
        private final int totalNumKeys;

        Compatible(
            String name,
            AggregatorFactories factories,
            List<QueryToFilterAdapter<?>> filters,
            boolean keyed,
            String otherBucketKey,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, filters, keyed, otherBucketKey, context, parent, cardinality, metadata);
            if (otherBucketKey == null) {
                this.totalNumKeys = filters.size();
            } else {
                this.totalNumKeys = filters.size() + 1;
            }
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            IntPredicate[] docFilters = new IntPredicate[filters().size()];
            for (int filterOrd = 0; filterOrd < filters().size(); filterOrd++) {
                docFilters[filterOrd] = filters().get(filterOrd).matchingDocIds(ctx); 
            }
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    boolean matched = false;
                    for (int i = 0; i < docFilters.length; i++) {
                        if (docFilters[i].test(doc)) {
                            collectBucket(sub, doc, bucketOrd(bucket, i));
                            matched = true;
                        }
                    }
                    if (otherBucketKey != null && false == matched) {
                        collectBucket(sub, doc, bucketOrd(bucket, docFilters.length));
                    }
                }
            };
        }

        final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
            return owningBucketOrdinal * totalNumKeys + filterOrd;
        }
    }

    /**
     * Counts collected documents, delegating to {@link DocCountProvider} for
     * how many documents each search hit is "worth".
     */
    static class Counter implements LeafCollector {
        final DocCountProvider docCount;
        private long count;

        Counter(DocCountProvider docCount) {
            this.docCount = docCount;
        }

        public long readAndReset(LeafReaderContext ctx) throws IOException {
            long result = count;
            count = 0;
            docCount.setLeafReaderContext(ctx);
            return result;
        }

        @Override
        public void collect(int doc) throws IOException {
            count += docCount.getDocCount(doc);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}
    }
}
