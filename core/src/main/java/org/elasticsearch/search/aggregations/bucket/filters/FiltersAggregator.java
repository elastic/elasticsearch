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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class FiltersAggregator extends BucketsAggregator {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");
    public static final ParseField OTHER_BUCKET_FIELD = new ParseField("other_bucket");
    public static final ParseField OTHER_BUCKET_KEY_FIELD = new ParseField("other_bucket_key");

    public static class KeyedFilter implements Writeable<KeyedFilter>, ToXContent {

        static final KeyedFilter PROTOTYPE = new KeyedFilter(null, null);
        private final String key;
        private final QueryBuilder<?> filter;

        public KeyedFilter(String key, QueryBuilder<?> filter) {
            this.key = key;
            this.filter = filter;
        }

        public String key() {
            return key;
        }

        public QueryBuilder<?> filter() {
            return filter;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(key, filter);
            return builder;
        }

        @Override
        public KeyedFilter readFrom(StreamInput in) throws IOException {
            String key = in.readString();
            QueryBuilder<?> filter = in.readQuery();
            return new KeyedFilter(key, filter);
    }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeQuery(filter);
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

    private final String[] keys;
    private Weight[] filters;
    private final boolean keyed;
    private final boolean showOtherBucket;
    private final String otherBucketKey;
    private final int totalNumKeys;

    public FiltersAggregator(String name, AggregatorFactories factories, String[] keys, Weight[] filters, boolean keyed, String otherBucketKey,
            AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.keyed = keyed;
        this.keys = keys;
        this.filters = filters;
        this.showOtherBucket = otherBucketKey != null;
        this.otherBucketKey = otherBucketKey;
        if (showOtherBucket) {
            this.totalNumKeys = keys.length + 1;
        } else {
            this.totalNumKeys = keys.length;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits[] bits = new Bits[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].scorer(ctx));
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
                if (showOtherBucket && !matched) {
                    collectBucket(sub, doc, bucketOrd(bucket, bits.length));
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(filters.length);
        for (int i = 0; i < keys.length; i++) {
            long bucketOrd = bucketOrd(owningBucketOrdinal, i);
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(keys[i], bucketDocCount(bucketOrd), bucketAggregations(bucketOrd), keyed);
            buckets.add(bucket);
        }
        // other bucket
        if (showOtherBucket) {
            long bucketOrd = bucketOrd(owningBucketOrdinal, keys.length);
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(otherBucketKey, bucketDocCount(bucketOrd),
                    bucketAggregations(bucketOrd), keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(filters.length);
        for (int i = 0; i < keys.length; i++) {
            InternalFilters.InternalBucket bucket = new InternalFilters.InternalBucket(keys[i], 0, subAggs, keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators(), metaData());
    }

    final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
        return owningBucketOrdinal * totalNumKeys + filterOrd;
    }

    public static class Factory extends AggregatorFactory<Factory> {

        private final List<KeyedFilter> filters;
        private final boolean keyed;
        private boolean otherBucket = false;
        private String otherBucketKey = "_other_";

        /**
         * @param name
         *            the name of this aggregation
         * @param filters
         *            the KeyedFilters to use with this aggregation.
         */
        public Factory(String name, List<KeyedFilter> filters) {
            super(name, InternalFilters.TYPE);
            this.filters = filters;
            this.keyed = true;
        }

        /**
         * @param name
         *            the name of this aggregation
         * @param filters
         *            the filters to use with this aggregation
         */
        public Factory(String name, QueryBuilder<?>... filters) {
            super(name, InternalFilters.TYPE);
            List<KeyedFilter> keyedFilters = new ArrayList<>(filters.length);
            for (int i = 0; i < filters.length; i++) {
                keyedFilters.add(new KeyedFilter(String.valueOf(i), filters[i]));
            }
            this.filters = keyedFilters;
            this.keyed = false;
        }

        /**
         * Set whether to include a bucket for documents not matching any filter
         */
        public Factory otherBucket(boolean otherBucket) {
            this.otherBucket = otherBucket;
            return this;
        }

        /**
         * Get whether to include a bucket for documents not matching any filter
         */
        public boolean otherBucket() {
            return otherBucket;
        }

        /**
         * Set the key to use for the bucket for documents not matching any
         * filter.
         */
        public Factory otherBucketKey(String otherBucketKey) {
            this.otherBucketKey = otherBucketKey;
            return this;
        }

        /**
         * Get the key to use for the bucket for documents not matching any
         * filter.
         */
        public String otherBucketKey() {
            return otherBucketKey;
        }

        // TODO: refactor in order to initialize the factory once with its parent,
        // the context, etc. and then have a no-arg lightweight create method
        // (since create may be called thousands of times)

        private IndexSearcher searcher;
        private String[] keys;
        private Weight[] weights;

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            IndexSearcher contextSearcher = context.searchContext().searcher();
            if (searcher != contextSearcher) {
                searcher = contextSearcher;
                weights = new Weight[filters.size()];
                keys = new String[filters.size()];
                for (int i = 0; i < filters.size(); ++i) {
                    KeyedFilter keyedFilter = filters.get(i);
                    this.keys[i] = keyedFilter.key;
                    Query filter = keyedFilter.filter.toFilter(context.searchContext().indexShard().getQueryShardContext());
                    this.weights[i] = contextSearcher.createNormalizedWeight(filter, false);
                }
            }
            return new FiltersAggregator(name, factories, keys, weights, keyed, otherBucket ? otherBucketKey : null, context, parent,
                    pipelineAggregators, metaData);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (keyed) {
                builder.startObject(FILTERS_FIELD.getPreferredName());
                for (KeyedFilter keyedFilter : filters) {
                    builder.field(keyedFilter.key(), keyedFilter.filter());
                }
                builder.endObject();
            } else {
                builder.startArray(FILTERS_FIELD.getPreferredName());
                for (KeyedFilter keyedFilter : filters) {
                    builder.value(keyedFilter.filter());
                }
                builder.endArray();
            }
            builder.field(OTHER_BUCKET_FIELD.getPreferredName(), otherBucket);
            builder.field(OTHER_BUCKET_KEY_FIELD.getPreferredName(), otherBucketKey);
            builder.endObject();
            return builder;
        }

        @Override
        protected Factory doReadFrom(String name, StreamInput in) throws IOException {
            Factory factory;
            if (in.readBoolean()) {
                int size = in.readVInt();
                List<KeyedFilter> filters = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    filters.add(KeyedFilter.PROTOTYPE.readFrom(in));
                }
                factory = new Factory(name, filters);
            } else {
                int size = in.readVInt();
                QueryBuilder<?>[] filters = new QueryBuilder<?>[size];
                for (int i = 0; i < size; i++) {
                    filters[i] = in.readQuery();
                }
                factory = new Factory(name, filters);
            }
            factory.otherBucket = in.readBoolean();
            factory.otherBucketKey = in.readString();
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeBoolean(keyed);
            if (keyed) {
                out.writeVInt(filters.size());
                for (KeyedFilter keyedFilter : filters) {
                    keyedFilter.writeTo(out);
                }
            } else {
                out.writeVInt(filters.size());
                for (KeyedFilter keyedFilter : filters) {
                    out.writeQuery(keyedFilter.filter());
                }
            }
            out.writeBoolean(otherBucket);
            out.writeString(otherBucketKey);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(filters, keyed, otherBucket, otherBucketKey);
        }

        @Override
        protected boolean doEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(filters, other.filters)
                    && Objects.equals(keyed, other.keyed)
                    && Objects.equals(otherBucket, other.otherBucket)
                    && Objects.equals(otherBucketKey, other.otherBucketKey);
        }
    }

}
