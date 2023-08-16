/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.BiConsumer;
import java.util.function.LongPredicate;

/**
 * Aggregator for {@code filters}. There are two known subclasses,
 * {@link FilterByFilterAggregator} which is fast but only works in some cases and
 * {@link Compatible} which works in all cases.
 * {@link FiltersAggregator#build} will build the fastest version that
 * works with the configuration.
 */
public abstract class FiltersAggregator extends BucketsAggregator {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");
    public static final ParseField OTHER_BUCKET_FIELD = new ParseField("other_bucket");
    public static final ParseField OTHER_BUCKET_KEY_FIELD = new ParseField("other_bucket_key");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");

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
            return Objects.equals(key, other.key) && Objects.equals(filter, other.filter);
        }
    }

    /**
     * Build an {@link Aggregator} for a {@code filters} aggregation. If there
     * isn't a parent, there aren't children, and we don't collect "other"
     * buckets then this will a faster {@link FilterByFilterAggregator} aggregator.
     * Otherwise it'll fall back to a slower aggregator that is
     * {@link Compatible} with parent, children, and "other" buckets.
     */
    public static FiltersAggregator build(
        String name,
        AggregatorFactories factories,
        List<QueryToFilterAdapter> filters,
        boolean keyed,
        String otherBucketKey,
        boolean keyedBucket,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        FilterByFilterAggregator.AdapterBuilder<FilterByFilterAggregator> filterByFilterBuilder =
            new FilterByFilterAggregator.AdapterBuilder<FilterByFilterAggregator>(
                name,
                keyed,
                keyedBucket,
                otherBucketKey,
                context,
                parent,
                cardinality,
                metadata
            ) {
                @Override
                protected FilterByFilterAggregator adapt(
                    CheckedFunction<AggregatorFactories, FilterByFilterAggregator, IOException> delegate
                ) throws IOException {
                    return delegate.apply(factories);
                }
            };
        for (QueryToFilterAdapter f : filters) {
            filterByFilterBuilder.add(f);
        }
        FilterByFilterAggregator filterByFilter = filterByFilterBuilder.build();
        if (filterByFilter != null) {
            return filterByFilter;
        }
        return new FiltersAggregator.Compatible(
            name,
            factories,
            filters,
            keyed,
            keyedBucket,
            otherBucketKey,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private final List<QueryToFilterAdapter> filters;
    private final boolean keyed;
    private final boolean keyedBucket;
    protected final String otherBucketKey;

    FiltersAggregator(
        String name,
        AggregatorFactories factories,
        List<QueryToFilterAdapter> filters,
        boolean keyed,
        boolean keyedBucket,
        String otherBucketKey,
        AggregationContext aggCtx,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggCtx, parent, cardinality.multiply(filters.size() + (otherBucketKey == null ? 0 : 1)), metadata);
        this.filters = List.copyOf(filters);
        this.keyed = keyed;
        this.keyedBucket = keyedBucket;
        this.otherBucketKey = otherBucketKey;
    }

    List<QueryToFilterAdapter> filters() {
        return filters;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            filters.size() + (otherBucketKey == null ? 0 : 1),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                if (offsetInOwningOrd < filters.size()) {
                    return new InternalFilters.InternalBucket(
                        filters.get(offsetInOwningOrd).key().toString(),
                        docCount,
                        subAggregationResults,
                        keyed,
                        keyedBucket
                    );
                }
                return new InternalFilters.InternalBucket(otherBucketKey, docCount, subAggregationResults, keyed, keyedBucket);
            },
            buckets -> new InternalFilters(name, buckets, keyed, keyedBucket, metadata())
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(filters.size() + (otherBucketKey == null ? 0 : 1));
        for (QueryToFilterAdapter filter : filters) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(
                filter.key().toString(),
                0,
                subAggs,
                keyed,
                keyedBucket
            );
            buckets.add(bucket);
        }

        if (otherBucketKey != null) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(otherBucketKey, 0, subAggs, keyed, keyedBucket);
            buckets.add(bucket);
        }

        return new InternalFilters(name, buckets, keyed, keyedBucket, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        List<Map<String, Object>> filtersDebug = new ArrayList<>(filters.size());
        for (QueryToFilterAdapter filter : filters) {
            Map<String, Object> debug = new HashMap<>();
            filter.collectDebugInfo(debug::put);
            filtersDebug.add(debug);
        }
        add.accept("filters", filtersDebug);
    }

    /**
     * Collects results by building a {@link LongPredicate} per filter and testing if
     * each doc sent to its {@link LeafBucketCollector} is in each filter
     * which is generally slower than {@link FilterByFilterAggregator} but is compatible
     * with parent and child aggregations.
     */
    static class Compatible extends FiltersAggregator {
        private final int totalNumKeys;

        Compatible(
            String name,
            AggregatorFactories factories,
            List<QueryToFilterAdapter> filters,
            boolean keyed,
            boolean keyedBucket,
            String otherBucketKey,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, filters, keyed, keyedBucket, otherBucketKey, context, parent, cardinality, metadata);
            if (otherBucketKey == null) {
                this.totalNumKeys = filters.size();
            } else {
                this.totalNumKeys = filters.size() + 1;
            }
        }

        @Override
        protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
            if (QueryToFilterAdapter.matchesNoDocs(filters()) && otherBucketKey == null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
            final DocIdSetIterator[] filteredDocIdSetIterators = new DocIdSetIterator[filters().size()];
            for (int filterOrd = 0; filterOrd < filters().size(); filterOrd++) {
                filteredDocIdSetIterators[filterOrd] = filters().get(filterOrd).matchingDocIdSetIterator(aggCtx.getLeafReaderContext());
            }

            final boolean useCompetitiveIterator = (parent == null && otherBucketKey == null);
            final int maxDocId = aggCtx.getLeafReaderContext().reader().maxDoc();

            // Tracks the last matched doc per filter.
            final int[] lastMatches = new int[filteredDocIdSetIterators.length];
            Arrays.fill(lastMatches, -1);

            /*
             * Even if the competitive iterator is applicable, it may not be used immediately.
             * We track the last doc id we've processed to effectively switch between the iterator and the direct use
             * of the filteredDocIdSetIterators in the leaf collector.
             */
            final DocId currentDocID = new DocId();
            currentDocID.set(-1);

            if (useCompetitiveIterator) {
                return new LeafBucketCollectorBase(sub, null) {

                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        // Check if the competitive iterator has matched the current doc.
                        if (doc == currentDocID.get()) {
                            for (int i = 0; i < filteredDocIdSetIterators.length; i++) {
                                if (lastMatches[i] == doc) {
                                    collectBucket(sub, doc, bucketOrd(bucket, i));
                                }
                            }
                        } else {
                            currentDocID.set(doc);
                            advanceAndCollectBuckets(doc, bucket, filteredDocIdSetIterators, lastMatches, sub);
                        }
                    }

                    @Override
                    public DocIdSetIterator competitiveIterator() {
                        return new CompetitiveIterator(filteredDocIdSetIterators, lastMatches, currentDocID, maxDocId);
                    }

                };
            }
            return new LeafBucketCollectorBase(sub, null) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    boolean matched = advanceAndCollectBuckets(doc, bucket, filteredDocIdSetIterators, lastMatches, sub);
                    if (otherBucketKey != null && matched == false) {
                        collectBucket(sub, doc, bucketOrd(bucket, filteredDocIdSetIterators.length));
                    }
                }
            };
        }

        private boolean advanceAndCollectBuckets(
            int doc,
            long bucket,
            DocIdSetIterator[] filteredDocIdSetIterators,
            int[] lastMatches,
            LeafBucketCollector sub
        ) throws IOException {
            boolean matched = false;
            for (int i = 0; i < filteredDocIdSetIterators.length; i++) {
                if (lastMatches[i] < doc) {
                    lastMatches[i] = filteredDocIdSetIterators[i].advance(doc);
                }
                if (lastMatches[i] == doc) {
                    collectBucket(sub, doc, bucketOrd(bucket, i));
                    matched = true;
                }
            }
            return matched;
        }

        final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
            return owningBucketOrdinal * totalNumKeys + filterOrd;
        }
    }

    /**
     * The competitive iterator wraps the per-filter iterators, skipping rangers with no filter matches during iteration.
     * This is achieved by storing the doc matches that each iterator returns in a priority queue that's
     * then used to provide the next possible matches during advancing.
     */
    private static class CompetitiveIterator extends DocIdSetIterator {

        final DocIdSetIterator[] filteredDocIdSetIterators;
        final int[] lastMatches;
        final DocId currentDocID;
        final int maxDocId;

        // Tracks next doc matches as they get returned from the per-filter iterators.
        final PriorityQueue<Integer> nextDocs;

        CompetitiveIterator(DocIdSetIterator[] filteredDocIdSetIterators, int[] lastMatches, DocId currentDocID, int maxDocId) {
            this.filteredDocIdSetIterators = filteredDocIdSetIterators;
            this.lastMatches = lastMatches;
            this.currentDocID = currentDocID;
            this.maxDocId = maxDocId;

            nextDocs = new PriorityQueue<>(filteredDocIdSetIterators.length) {
                @Override
                public boolean offer(Integer e) {
                    // Skip inserting duplicate values.
                    if (contains(e)) {
                        return false;
                    }
                    return super.offer(e);
                }
            };
        }

        /**
         * Returns the next possible filter match, starting from the passed target doc.
         * It uses the priority queue to determine if it can skip ranges with no matches.
         */
        private int getNextDoc(int target) {
            if (nextDocs.isEmpty()) {
                return target;
            }

            // If the next doc in the priority queue is further ahead, skip the rest.
            int nextDoc = nextDocs.poll();
            if (nextDoc > target) {
                return nextDoc;
            }

            // There's at least one doc in the priority queue that's not further ahead so we can't skip
            // any docs. Remove any past docs from the priority queue, not needed any more.
            while (nextDocs.isEmpty() == false && nextDocs.peek() <= target) {
                nextDocs.poll();
            }
            return target;
        }

        @Override
        public int docID() {
            return currentDocID.get();
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(currentDocID.get() + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            target = getNextDoc(target);
            if (target >= maxDocId) {
                target = DocIdSetIterator.NO_MORE_DOCS;
                currentDocID.set(target);
                return target;
            }
            for (int i = 0; i < filteredDocIdSetIterators.length; i++) {
                if (lastMatches[i] < target) {
                    lastMatches[i] = filteredDocIdSetIterators[i].advance(target);
                    nextDocs.add(lastMatches[i]);
                }
            }
            currentDocID.set(target);
            return target;
        }

        @Override
        public long cost() {
            return maxDocId;
        }
    }

    /**
     * Wrapper for doc value (integer), so that it can be updated by different classes.
     */
    private static class DocId {
        private int doc;

        void set(int value) {
            this.doc = value;
        }

        int get() {
            return doc;
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
