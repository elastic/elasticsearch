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
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregate all docs that match a filter.
 */
public class FilterAggregator extends SingleBucketAggregator {

    private final Weight filter;

    public FilterAggregator(String name,
                            Weight filter,
                            AggregatorFactories factories,
                            AggregationContext aggregationContext,
                            Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                            Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.filter = filter;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filter.scorer(ctx));
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bits.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalFilter(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

    public static class FilterAggregatorBuilder extends AggregatorBuilder<FilterAggregatorBuilder> {

        private QueryBuilder<?> filter;

        /**
         * @param name
         *            the name of this aggregation
         * @param filter
         *            Set the filter to use, only documents that match this
         *            filter will fall into the bucket defined by this
         *            {@link Filter} aggregation.
         */
        public FilterAggregatorBuilder(String name, QueryBuilder<?> filter) {
            super(name, InternalFilter.TYPE);
            this.filter = filter;
        }

        @Override
        protected AggregatorFactory<?> doBuild(AggregationContext context, AggregatorFactory<?> parent,
                AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
            return new FilterAggregatorFactory(name, type, filter, context, parent, subFactoriesBuilder, metaData);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            if (filter != null) {
                filter.toXContent(builder, params);
            }
            return builder;
        }

        @Override
        protected FilterAggregatorBuilder doReadFrom(String name, StreamInput in) throws IOException {
            FilterAggregatorBuilder factory = new FilterAggregatorBuilder(name, in.readQuery());
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeQuery(filter);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(filter);
        }

        @Override
        protected boolean doEquals(Object obj) {
            FilterAggregatorBuilder other = (FilterAggregatorBuilder) obj;
            return Objects.equals(filter, other.filter);
        }

    }
}


