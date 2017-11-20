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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Aggregation for adjacency matrices.
 *
 * NOTE! This is an experimental class.
 *
 * TODO the aggregation produces a sparse response but in the
 * computation it uses a non-sparse structure (an array of Bits
 * objects). This could be changed to a sparse structure in future.
 *
 */
public class AdjacencyMatrixAggregator extends BucketsAggregator {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");

    protected static class KeyedFilter implements Writeable, ToXContentFragment {
        private final String key;
        private final QueryBuilder filter;

        public static final NamedObjectParser<KeyedFilter, Void> PARSER =
                (XContentParser p, Void c, String name) ->
                     new KeyedFilter(name, parseInnerQueryBuilder(p));

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

    private final String[] keys;
    private Weight[] filters;
    private final int totalNumKeys;
    private final int totalNumIntersections;
    private final String separator;

    public AdjacencyMatrixAggregator(String name, AggregatorFactories factories, String separator, String[] keys,
            Weight[] filters, SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.separator = separator;
        this.keys = keys;
        this.filters = filters;
        this.totalNumIntersections = ((keys.length * keys.length) - keys.length) / 2;
        this.totalNumKeys = keys.length + totalNumIntersections;
    }

    private static class BitsIntersector implements Bits {
        Bits a;
        Bits b;

        BitsIntersector(Bits a, Bits b) {
            super();
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean get(int index) {
            return a.get(index) && b.get(index);
        }

        @Override
        public int length() {
            return Math.min(a.length(), b.length());
        }

    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits[] bits = new Bits[filters.length + totalNumIntersections];
        for (int i = 0; i < filters.length; ++i) {
            bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].scorerSupplier(ctx));
        }
        // Add extra Bits for intersections
        int pos = filters.length;
        for (int i = 0; i < filters.length; i++) {
            for (int j = i + 1; j < filters.length; j++) {
                bits[pos++] = new BitsIntersector(bits[i], bits[j]);
            }
        }
        assert pos == bits.length;
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                for (int i = 0; i < bits.length; i++) {
                    if (bits[i].get(doc)) {
                        collectBucket(sub, doc, bucketOrd(bucket, i));
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {

        // Buckets are ordered into groups - [keyed filters] [key1&key2 intersects]

        List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>(filters.length);
        for (int i = 0; i < keys.length; i++) {
            long bucketOrd = bucketOrd(owningBucketOrdinal, i);
            int docCount = bucketDocCount(bucketOrd);
            // Empty buckets are not returned because this aggregation will commonly be used under a
            // a date-histogram where we will look for transactions over time and can expect many
            // empty buckets.
            if (docCount > 0) {
                InternalAdjacencyMatrix.InternalBucket bucket = new InternalAdjacencyMatrix.InternalBucket(keys[i],
                        docCount, bucketAggregations(bucketOrd));
                buckets.add(bucket);
            }
        }
        int pos = keys.length;
        for (int i = 0; i < keys.length; i++) {
            for (int j = i + 1; j < keys.length; j++) {
                long bucketOrd = bucketOrd(owningBucketOrdinal, pos);
                int docCount = bucketDocCount(bucketOrd);
                // Empty buckets are not returned due to potential for very sparse matrices
                if (docCount > 0) {
                    String intersectKey = keys[i] + separator + keys[j];
                    InternalAdjacencyMatrix.InternalBucket bucket = new InternalAdjacencyMatrix.InternalBucket(intersectKey,
                            docCount, bucketAggregations(bucketOrd));
                    buckets.add(bucket);
                }
                pos++;
            }
        }
        return new InternalAdjacencyMatrix(name, buckets, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>(0);
        return new InternalAdjacencyMatrix(name, buckets, pipelineAggregators(), metaData());
    }

    final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
        return owningBucketOrdinal * totalNumKeys + filterOrd;
    }

}
