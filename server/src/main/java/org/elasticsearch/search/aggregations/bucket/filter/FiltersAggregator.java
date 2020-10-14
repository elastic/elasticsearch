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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
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

import static java.util.Arrays.compareUnsigned;

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
        FiltersAggregator filterOrder = filterOrderOrNull(
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

    private static FiltersAggregator filterOrderOrNull(
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

    public FiltersAggregator(String name, AggregatorFactories factories, String[] keys, boolean keyed,
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

    public abstract boolean collectsInFilterOrder();

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

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            if (filterWeights == null) {
                filterWeights = buildWeights(context.query(), filters);
            }
            Bits live = ctx.reader().getLiveDocs();
            for (int filterOrd = 0; filterOrd < filters.length; filterOrd++) {
                Scorer scorer = filterWeights[filterOrd].scorer(ctx);
                if (scorer == null) {
                    // the filter doesn't match any docs
                    continue;
                }
                DocIdSetIterator itr = scorer.iterator();
                if (live == null) {
                    while (itr.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        collectBucket(sub, itr.docID(), filterOrd);
                    }
                } else {
                    segmentsWithDeletedDocs++;
                    while (itr.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        if (live.get(itr.docID())) {
                            collectBucket(sub, itr.docID(), filterOrd);
                        }
                    }
                }
            }
            throw new CollectionTerminatedException();
        }

        @Override
        public boolean collectsInFilterOrder() {
            return true;
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

        @Override
        public boolean collectsInFilterOrder() {
            return false;
        }
    }

    protected Weight[] buildWeights(Query topLevelQuery, Query filters[]) throws IOException{
        Weight[] weights = new Weight[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            Query filter = filterMatchingBoth(topLevelQuery, filters[i]);
            weights[i] = context.searcher().createWeight(context.searcher().rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1);
        }
        return weights;
    }

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
            PointRangeQuery merged = mergePointRangeQueries((PointRangeQuery) unwrappedLhs, (PointRangeQuery) unwrappedRhs);
            if (merged != null) {
                // TODO rewrap?
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

    private PointRangeQuery mergePointRangeQueries(PointRangeQuery lhs, PointRangeQuery rhs) {
        if (lhs.getField() != rhs.getField() || lhs.getNumDims() != rhs.getNumDims() || lhs.getBytesPerDim() != rhs.getBytesPerDim()) {
            return null;
        }
        byte[] lower = mergePoint(lhs.getLowerPoint(), rhs.getLowerPoint(), lhs.getNumDims(), lhs.getBytesPerDim(), true);
        if (lower == null) {
            return null;
        }
        byte[] upper = mergePoint(lhs.getUpperPoint(), rhs.getUpperPoint(), lhs.getNumDims(), lhs.getBytesPerDim(), false);
        if (upper == null) {
            return null;
        }
        return new PointRangeQuery(lhs.getField(), lower, upper, lhs.getNumDims()) {
            @Override
            protected String toString(int dimension, byte[] value) {
                // Stolen from Lucene's Binary range query. It'd be best to delegate, but the method isn't visible.
                StringBuilder sb = new StringBuilder();
                sb.append("binary(");
                for (int i = 0; i < value.length; i++) {
                    if (i > 0) {
                        sb.append(' ');
                    }
                    sb.append(Integer.toHexString(value[i] & 0xFF));
                }
                sb.append(')');
                return sb.toString();
            }
        };
    }

    /**
     * Figure out if lhs's lower point is lower in all dimensions than
     * rhs's lower point or if it is further. Return null if it is closer
     * in some dimensions and further in others.
     */
    private byte[] mergePoint(byte[] lhs, byte[] rhs, int numDims, int bytesPerDim, boolean mergingLower) {
        int runningCmp = 0;
        for (int dim = 0; dim < numDims; dim++) {
            int cmp = cmpDim(lhs, rhs, dim, bytesPerDim);
            if (runningCmp == 0) {
                // Previous dimensions were all equal
                runningCmp = cmp;
                continue;
            }
            if (cmp == 0) {
                // This dimension has the same value.
                continue;
            }
            if ((runningCmp ^ cmp) < 0) {
                // Signs differ so this dimension doesn't compare the same way as the previous ones so we can't merge.
                return null;
            }
        }
        if (runningCmp < 0) {
            // lhs is lower
            return mergingLower ? rhs : lhs;
        }
        return mergingLower ? lhs : rhs;
    }

    private int cmpDim(byte[] lhs, byte[] rhs, int dim, int bytesPerDim) {
        int offset = dim * bytesPerDim;
        return compareUnsigned(lhs, offset, offset + bytesPerDim, rhs, offset, offset + bytesPerDim);
    }
}
