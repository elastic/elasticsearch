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
package org.elasticsearch.search.aggregations;

import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private static Aggregator createAndRegisterContextAware(AggregationContext context, AggregatorFactory factory, Aggregator parent, long estimatedBucketsCount) {
        final Aggregator aggregator = factory.create(context, parent, estimatedBucketsCount);
        if (aggregator.shouldCollect()) {
            context.registerReaderContextAware(aggregator);
        }
        return aggregator;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple buckets.
     */
    public Aggregator[] createSubAggregators(Aggregator parent, final long estimatedBucketsCount) {
        Aggregator[] aggregators = new Aggregator[count()];
        for (int i = 0; i < factories.length; ++i) {
            final AggregatorFactory factory = factories[i];
            final Aggregator first = createAndRegisterContextAware(parent.context(), factory, parent, estimatedBucketsCount);
            if (first.bucketAggregationMode() == BucketAggregationMode.MULTI_BUCKETS) {
                // This aggregator already supports multiple bucket ordinals, can be used directly
                aggregators[i] = first;
                continue;
            }
            // the aggregator doesn't support multiple ordinals, let's wrap it so that it does.
            aggregators[i] = new Aggregator(first.name(), BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, 1, first.context(), first.parent()) {

                ObjectArray<Aggregator> aggregators;

                {
                    // if estimated count is zero, we at least create a single aggregator.
                    // The estimated count is just an estimation and we can't rely on how it's estimated (that is, an
                    // estimation of 0 should not imply that we'll end up without any buckets)
                    long arraySize = estimatedBucketsCount > 0 ?  estimatedBucketsCount : 1;
                    aggregators = bigArrays.newObjectArray(arraySize);
                    aggregators.set(0, first);
                    for (long i = 1; i < arraySize; ++i) {
                        aggregators.set(i, createAndRegisterContextAware(parent.context(), factory, parent, estimatedBucketsCount));
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
                    aggregators = bigArrays.grow(aggregators, owningBucketOrdinal + 1);
                    Aggregator aggregator = aggregators.get(owningBucketOrdinal);
                    if (aggregator == null) {
                        aggregator = createAndRegisterContextAware(parent.context(), factory, parent, estimatedBucketsCount);
                        aggregators.set(owningBucketOrdinal, aggregator);
                    }
                    aggregator.collect(doc, 0);
                }

                @Override
                public void setNextReader(AtomicReaderContext reader) {
                }

                @Override
                public InternalAggregation buildAggregation(long owningBucketOrdinal) {
                    // The bucket ordinal may be out of range in case of eg. a terms/filter/terms where
                    // the filter matches no document in the highest buckets of the first terms agg
                    if (owningBucketOrdinal >= aggregators.size() || aggregators.get(owningBucketOrdinal) == null) {
                        return first.buildEmptyAggregation();
                    } else {
                        return aggregators.get(owningBucketOrdinal).buildAggregation(0);
                    }
                }

                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return first.buildEmptyAggregation();
                }

                @Override
                public void doRelease() {
                    final Iterable<Aggregator> aggregatorsIter = new Iterable<Aggregator>() {

                        @Override
                        public Iterator<Aggregator> iterator() {
                            return new UnmodifiableIterator<Aggregator>() {

                                long i = 0;

                                @Override
                                public boolean hasNext() {
                                    return i < aggregators.size();
                                }

                                @Override
                                public Aggregator next() {
                                    return aggregators.get(i++);
                                }

                            };
                        }

                    };
                    Releasables.release(Iterables.concat(aggregatorsIter, Collections.singleton(aggregators)));
                }
            };
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            aggregators[i] = createAndRegisterContextAware(ctx, factories[i], null, 0);
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

        private final Set<String> names = new HashSet<String>();
        private final List<AggregatorFactory> factories = new ArrayList<AggregatorFactory>();

        public Builder add(AggregatorFactory factory) {
            if (!names.add(factory.name)) {
                throw new ElasticsearchIllegalArgumentException("Two sibling aggregations cannot have the same name: [" + factory.name + "]");
            }
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
