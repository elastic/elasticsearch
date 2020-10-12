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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
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
import java.util.function.Supplier;

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
        Supplier<Weight[]> filtersSupplier,
        boolean keyed,
        String otherBucketKey,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        FiltersAggregator filterOrder = filterOrderAggregatorOrNull(
            name,
            factories,
            keys,
            filtersSupplier,
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
        return new StandardOrderAggregator(
            name,
            factories,
            keys,
            filtersSupplier,
            keyed,
            otherBucketKey,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private static FiltersAggregator filterOrderAggregatorOrNull(
        String name,
        AggregatorFactories factories,
        String[] keys,
        Supplier<Weight[]> filtersSupplier,
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
        // NOCOMMIT this is only ok if there isn't a query or if the filter is "inside" the query already
        return new FilterOrderAggregator(
            name,
            factories,
            keys,
            filtersSupplier,
            keyed,
            otherBucketKey,
            context,
            parent,
            cardinality,
            metadata
        );
    }


    private final String[] keys;
    private final Supplier<Weight[]> filtersSupplier;
    private final boolean keyed;
    protected final String otherBucketKey;
    private Weight[] filters;

    public FiltersAggregator(String name, AggregatorFactories factories, String[] keys, Supplier<Weight[]> filtersSupplier, boolean keyed,
            String otherBucketKey, SearchContext context, Aggregator parent, CardinalityUpperBound cardinality,
            Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, cardinality.multiply(keys.length + (otherBucketKey == null ? 0 : 1)), metadata);
        this.keyed = keyed;
        this.keys = keys;
        this.filtersSupplier = filtersSupplier;
        this.otherBucketKey = otherBucketKey;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (filters == null) {
            filters = this.filtersSupplier.get();
        }
        return getLeafCollector(ctx, sub, filters);
    }

    protected abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub, Weight[] filters)
        throws IOException;

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

    private static class FilterOrderAggregator extends FiltersAggregator {
        public FilterOrderAggregator(
            String name,
            AggregatorFactories factories,
            String[] keys,
            Supplier<Weight[]> filtersSupplier,
            boolean keyed,
            String otherBucketKey,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, keys, filtersSupplier, keyed, otherBucketKey, context, parent, cardinality, metadata);
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub, Weight[] filters)
            throws IOException {
            Bits live = ctx.reader().getLiveDocs();
            for (int filterOrd = 0; filterOrd < filters.length; filterOrd++) {
                DocIdSetIterator itr = filters[filterOrd].scorer(ctx).iterator();
                if (live == null) {
                    while (itr.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        collectBucket(sub, itr.docID(), filterOrd);
                    }
                } else {
                    while (itr.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        if (live.get(itr.docID())) {
                            collectBucket(sub, itr.docID(), filterOrd);
                        }
                    }
                }
            }
            throw new CollectionTerminatedException();
        }
    }

    private static class StandardOrderAggregator extends FiltersAggregator {
        private final int totalNumKeys;

        public StandardOrderAggregator(
            String name,
            AggregatorFactories factories,
            String[] keys,
            Supplier<Weight[]> filtersSupplier,
            boolean keyed,
            String otherBucketKey,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, keys, filtersSupplier, keyed, otherBucketKey, context, parent, cardinality, metadata);
            if (otherBucketKey == null) {
                this.totalNumKeys = keys.length;
            } else {
                this.totalNumKeys = keys.length + 1;
            }
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub, Weight[] filters)
            throws IOException {
            final Bits[] bits = new Bits[filters.length];
            for (int i = 0; i < filters.length; ++i) {
                bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].scorerSupplier(ctx));
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
}
