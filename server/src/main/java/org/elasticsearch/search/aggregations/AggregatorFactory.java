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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AggregatorFactory {

    public static final class MultiBucketAggregatorWrapper extends Aggregator {
        private final BigArrays bigArrays;
        private final Aggregator parent;
        private final AggregatorFactory factory;
        private final Aggregator first;
        ObjectArray<Aggregator> aggregators;
        ObjectArray<LeafBucketCollector> collectors;

        MultiBucketAggregatorWrapper(BigArrays bigArrays, SearchContext context, Aggregator parent, AggregatorFactory factory,
                Aggregator first) {
            this.bigArrays = bigArrays;
            this.parent = parent;
            this.factory = factory;
            this.first = first;
            context.addReleasable(this, Lifetime.PHASE);
            aggregators = bigArrays.newObjectArray(1);
            aggregators.set(0, first);
            collectors = bigArrays.newObjectArray(1);
        }

        public Class<?> getWrappedClass() {
            return first.getClass();
        }

        @Override
        public String name() {
            return first.name();
        }

        @Override
        public SearchContext context() {
            return first.context();
        }

        @Override
        public Aggregator parent() {
            return first.parent();
        }

        @Override
        public ScoreMode scoreMode() {
            return first.scoreMode();
        }

        @Override
        public Aggregator subAggregator(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void preCollection() throws IOException {
            for (long i = 0; i < aggregators.size(); ++i) {
                final Aggregator aggregator = aggregators.get(i);
                if (aggregator != null) {
                    aggregator.preCollection();
                }
            }
        }

        @Override
        public void postCollection() throws IOException {
            for (long i = 0; i < aggregators.size(); ++i) {
                final Aggregator aggregator = aggregators.get(i);
                if (aggregator != null) {
                    aggregator.postCollection();
                }
            }
        }

        @Override
        public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx) {
            for (long i = 0; i < collectors.size(); ++i) {
                collectors.set(i, null);
            }
            return new LeafBucketCollector() {
                Scorable scorer;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    this.scorer = scorer;
                }

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    collectors = bigArrays.grow(collectors, bucket + 1);

                    LeafBucketCollector collector = collectors.get(bucket);
                    if (collector == null) {
                        aggregators = bigArrays.grow(aggregators, bucket + 1);
                        Aggregator aggregator = aggregators.get(bucket);
                        if (aggregator == null) {
                            aggregator = factory.create(parent, true);
                            aggregator.preCollection();
                            aggregators.set(bucket, aggregator);
                        }
                        collector = aggregator.getLeafCollector(ctx);
                        if (scorer != null) {
                            // Passing a null scorer can cause unexpected NPE at a later time,
                            // which can't not be directly linked to the fact that a null scorer has been supplied.
                            collector.setScorer(scorer);
                        }
                        collectors.set(bucket, collector);
                    }
                    collector.collect(doc, 0);
                }

            };
        }

        @Override
        public InternalAggregation buildAggregation(long bucket) throws IOException {
            if (bucket < aggregators.size()) {
                Aggregator aggregator = aggregators.get(bucket);
                if (aggregator != null) {
                    return aggregator.buildAggregation(0);
                }
            }
            return buildEmptyAggregation();
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return first.buildEmptyAggregation();
        }

        @Override
        public void close() {
            Releasables.close(aggregators, collectors);
        }
    }

    protected final String name;
    protected final AggregatorFactory parent;
    protected final AggregatorFactories factories;
    protected final Map<String, Object> metaData;
    protected final SearchContext context;

    /**
     * Constructs a new aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @throws IOException
     *             if an error occurs creating the factory
     */
    public AggregatorFactory(String name, SearchContext context, AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        this.name = name;
        this.context = context;
        this.parent = parent;
        this.factories = subFactoriesBuilder.build(context, this);
        this.metaData = metaData;
    }

    public String name() {
        return name;
    }

    public void doValidate() {
    }

    protected abstract Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

    /**
     * Creates the aggregator
     *
     * @param parent
     *            The parent aggregator (if this is a top level factory, the
     *            parent will be {@code null})
     * @param collectsFromSingleBucket
     *            If true then the created aggregator will only be collected
     *            with {@code 0} as a bucket ordinal. Some factories can take
     *            advantage of this in order to return more optimized
     *            implementations.
     *
     * @return The created aggregator
     */
    public final Aggregator create(Aggregator parent, boolean collectsFromSingleBucket) throws IOException {
        return createInternal(parent, collectsFromSingleBucket, this.factories.createPipelineAggregators(), this.metaData);
    }

    public AggregatorFactory getParent() {
        return parent;
    }

    /**
     * Utility method. Given an {@link AggregatorFactory} that creates
     * {@link Aggregator}s that only know how to collect bucket {@code 0}, this
     * returns an aggregator that can collect any bucket.
     */
    protected static Aggregator asMultiBucketAggregator(final AggregatorFactory factory, final SearchContext context,
            final Aggregator parent) throws IOException {
        final Aggregator first = factory.create(parent, true);
        final BigArrays bigArrays = context.bigArrays();
        return new MultiBucketAggregatorWrapper(bigArrays, context, parent, factory, first);
    }

}
