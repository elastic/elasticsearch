/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
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
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

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
        String[] keys,
        Query[] filters,
        boolean keyed,
        String otherBucketKey,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        FiltersAggregator filterOrder = buildFilterOrderOrNull(
            name,
            factories,
            keys,
            filters,
            keyed,
            otherBucketKey,
            context,
            parent,
            cardinality,
            metadata
        );
        if (filterOrder != null) {
            return filterOrder;
        }
        return new FiltersAggregator.Compatible(
            name,
            factories,
            keys,
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
     * Build an {@link Aggregator} for a {@code filters} aggregation if we
     * can collect {@link FilterByFilter}, otherwise return {@code null}. We can
     * collect filter by filter if there isn't a parent, there aren't children,
     * and we don't collect "other" buckets. Collecting {@link FilterByFilter}
     * is generally going to be much faster than the {@link Compatible} aggregator.
     */
    public static FiltersAggregator buildFilterOrderOrNull(
        String name,
        AggregatorFactories factories,
        String[] keys,
        Query[] filters,
        boolean keyed,
        String otherBucketKey,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (parent != null) {
            return null;
        }
        if (factories.countAggregators() != 0) {
            return null;
        }
        if (otherBucketKey != null) {
            return null;
        }
        return new FiltersAggregator.FilterByFilter(
            name,
            keys,
            filters,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private final String[] keys;
    private final boolean keyed;
    protected final String otherBucketKey;

    private FiltersAggregator(String name, AggregatorFactories factories, String[] keys, boolean keyed,
            String otherBucketKey, SearchContext context, Aggregator parent, CardinalityUpperBound cardinality,
            Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, cardinality.multiply(keys.length + (otherBucketKey == null ? 0 : 1)), metadata);
        this.keyed = keyed;
        this.keys = keys;
        this.otherBucketKey = otherBucketKey;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(owningBucketOrds, keys.length + (otherBucketKey == null ? 0 : 1),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                if (offsetInOwningOrd < keys.length) {
                    return new InternalFilters.InternalBucket(keys[offsetInOwningOrd], docCount,
                            subAggregationResults, keyed);
                }
                return new InternalFilters.InternalBucket(otherBucketKey, docCount, subAggregationResults, keyed);
            }, buckets -> new InternalFilters(name, buckets, keyed, metadata())); 
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(keys[i], 0, subAggs, keyed);
            buckets.add(bucket);
        }

        if (otherBucketKey != null) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(otherBucketKey, 0, subAggs, keyed);
            buckets.add(bucket);
        }

        return new InternalFilters(name, buckets, keyed, metadata());
    }

    /**
     * Collects results by running each filter against the searcher and doesn't
     * build any {@link LeafBucketCollector}s which is generally faster than
     * {@link Compatible} but doesn't support when there is a parent aggregator
     * or any child aggregators.
     */
    private static class FilterByFilter extends FiltersAggregator {
        private final Query[] filters;
        private Weight[] filterWeights;
        private int segmentsWithDeletedDocs;

        FilterByFilter(
            String name,
            String[] keys,
            Query[] filters,
            boolean keyed,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, AggregatorFactories.EMPTY, keys, keyed, null, context, parent, cardinality, metadata);
            this.filters = filters;
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
            if (filterWeights == null) {
                filterWeights = buildWeights(topLevelQuery(), filters);
            }
            Bits live = ctx.reader().getLiveDocs();
            for (int filterOrd = 0; filterOrd < filters.length; filterOrd++) {
                BulkScorer scorer = filterWeights[filterOrd].bulkScorer(ctx);
                if (scorer == null) {
                    // the filter doesn't match any docs
                    continue;
                }
                TotalHitCountCollector collector = new TotalHitCountCollector();
                scorer.score(collector, live);
                incrementBucketDocCount(filterOrd, collector.getTotalHits());
            }
            // Throwing this exception is how we communicate to the collection mechanism that we don't need the segment.
            throw new CollectionTerminatedException();
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("segments_with_deleted_docs", segmentsWithDeletedDocs);
        }
    }

    /**
     * Collects results by building a {@link Bits} per filter and testing if
     * each doc sent to its {@link LeafBucketCollector} is in each filter
     * which is generally slower than {@link FilterByFilter} but is compatible
     * with parent and child aggregations.
     */
    private static class Compatible extends FiltersAggregator {
        private final Query[] filters;
        private Weight[] filterWeights;

        private final int totalNumKeys;

        Compatible(
            String name,
            AggregatorFactories factories,
            String[] keys,
            Query[] filters,
            boolean keyed,
            String otherBucketKey,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, keys, keyed, otherBucketKey, context, parent, cardinality, metadata);
            this.filters = filters;
            if (otherBucketKey == null) {
                this.totalNumKeys = keys.length;
            } else {
                this.totalNumKeys = keys.length + 1;
            }
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            if (filterWeights == null) {
                filterWeights = buildWeights(new MatchAllDocsQuery(), filters);
            }
            final Bits[] bits = new Bits[filters.length];
            for (int i = 0; i < filters.length; ++i) {
                bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filterWeights[i].scorerSupplier(ctx));
            }
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    boolean matched = false;
                    for (int i = 0; i < bits.length; i++) {
                        if (bits[i].get(doc)) {
                            collectBucket(sub, doc, bucketOrd(bucket, i));
                            matched = true;
                        }
                    }
                    if (otherBucketKey != null && false == matched) {
                        collectBucket(sub, doc, bucketOrd(bucket, bits.length));
                    }
                }
            };
        }

        final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
            return owningBucketOrdinal * totalNumKeys + filterOrd;
        }
    }

    protected Weight[] buildWeights(Query topLevelQuery, Query filters[]) throws IOException{
        Weight[] weights = new Weight[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            Query filter = filterMatchingBoth(topLevelQuery, filters[i]);
            weights[i] = searcher().createWeight(searcher().rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1);
        }
        return weights;
    }

    /**
     * Make a filter that matches both queries, merging the
     * {@link PointRangeQuery}s together if possible. The "merging together"
     * part is provides a fairly substantial speed boost then executing a
     * top level query on a date and a filter on a date. This kind of thing
     * is very common when visualizing logs and metrics.
     */
    private Query filterMatchingBoth(Query lhs, Query rhs) {
        if (lhs instanceof MatchAllDocsQuery) {
            return rhs;
        }
        if (rhs instanceof MatchAllDocsQuery) {
            return lhs;
        }
        Query unwrappedLhs = unwrap(lhs);
        Query unwrappedRhs = unwrap(rhs);
        if (unwrappedLhs instanceof PointRangeQuery && unwrappedRhs instanceof PointRangeQuery) {
            Query merged = MergedPointRangeQuery.merge((PointRangeQuery) unwrappedLhs, (PointRangeQuery) unwrappedRhs);
            if (merged != null) {
                // Should we rewrap here?
                return merged;
            }
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(lhs, BooleanClause.Occur.MUST);
        builder.add(rhs, BooleanClause.Occur.MUST);
        return builder.build();
    }

    private Query unwrap(Query query) {
        if (query instanceof IndexSortSortedNumericDocValuesRangeQuery) {
            query = ((IndexSortSortedNumericDocValuesRangeQuery) query).getFallbackQuery();
        }
        if (query instanceof IndexOrDocValuesQuery) {
            query = ((IndexOrDocValuesQuery) query).getIndexQuery();
        }
        return query;
    }
}
