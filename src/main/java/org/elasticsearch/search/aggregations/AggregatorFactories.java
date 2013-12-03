/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AggregatorFactories {

    public static final AggregatorFactories EMPTY = new Empty();

    private final AggregatorFactory[] factories;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory[] factories) {
        this.factories = factories;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple buckets.
     */
    public Aggregator[] createSubAggregators(Aggregator parent, final long estimatedBucketsCount) {
        Aggregator[] aggregators = new Aggregator[count()];
        for (int i = 0; i < factories.length; ++i) {
            final AggregatorFactory factory = factories[i];
            final Aggregator first = factory.create(parent.context(), parent, estimatedBucketsCount);
            if (first.bucketAggregationMode() == BucketAggregationMode.MULTI_BUCKETS) {
                // This aggregator already supports multiple bucket ordinals, can be used directly
                aggregators[i] = first;
                continue;
            }
            // the aggregator doesn't support multiple ordinals, let's wrap it so that it does.
            aggregators[i] = new Aggregator(first.name(), BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, 1, first.context(), first.parent()) {

                ObjectArray<Aggregator> aggregators;

                {
                    aggregators = BigArrays.newObjectArray(estimatedBucketsCount, context.pageCacheRecycler());
                    aggregators.set(0, first);
                    for (long i = 1; i < estimatedBucketsCount; ++i) {
                        aggregators.set(i, factory.create(parent.context(), parent, estimatedBucketsCount));
                    }
                }

                @Override
                public boolean shouldCollect() {
                    return first.shouldCollect();
                }

                @Override
                protected void doPostCollection() {
                    for (long i = 0; i < aggregators.size(); ++i) {
                        final Aggregator aggregator = aggregators.get(i);
                        if (aggregator != null) {
                            aggregator.postCollection();
                        }
                    }
                }

                @Override
                public void collect(int doc, long owningBucketOrdinal) throws IOException {
                    aggregators = BigArrays.grow(aggregators, owningBucketOrdinal + 1);
                    Aggregator aggregator = aggregators.get(owningBucketOrdinal);
                    if (aggregator == null) {
                        aggregator = factory.create(parent.context(), parent, estimatedBucketsCount);
                        aggregators.set(owningBucketOrdinal, aggregator);
                    }
                    aggregator.collect(doc, 0);
                }

                @Override
                public InternalAggregation buildAggregation(long owningBucketOrdinal) {
                    return aggregators.get(owningBucketOrdinal).buildAggregation(0);
                }

                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return first.buildEmptyAggregation();
                }

                @Override
                public void doRelease() {
                    Releasables.release(aggregators);
                }
            };
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            aggregators[i] = factories[i].create(ctx, null, 0);
        }
        return aggregators;
    }

    public int count() {
        return factories.length;
    }

    void setParent(AggregatorFactory parent) {
        for (AggregatorFactory factory : factories) {
            factory.parent = parent;
        }
    }

    public void validate() {
        for (AggregatorFactory factory : factories) {
            factory.validate();
        }
    }

    private final static class Empty extends AggregatorFactories {

        private static final AggregatorFactory[] EMPTY_FACTORIES = new AggregatorFactory[0];
        private static final Aggregator[] EMPTY_AGGREGATORS = new Aggregator[0];

        private Empty() {
            super(EMPTY_FACTORIES);
        }

        @Override
        public Aggregator[] createSubAggregators(Aggregator parent, long estimatedBucketsCount) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
            return EMPTY_AGGREGATORS;
        }

    }

    public static class Builder {

        private List<AggregatorFactory> factories = new ArrayList<AggregatorFactory>();

        public Builder add(AggregatorFactory factory) {
            factories.add(factory);
            return this;
        }

        public AggregatorFactories build() {
            if (factories.isEmpty()) {
                return EMPTY;
            }
            return new AggregatorFactories(factories.toArray(new AggregatorFactory[factories.size()]));
        }
    }
}
