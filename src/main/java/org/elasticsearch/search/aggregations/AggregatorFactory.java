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
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregatorFactory {

    protected String name;
    protected String type;
    protected AggregatorFactory parent;
    protected AggregatorFactories factories = AggregatorFactories.EMPTY;
    protected Map<String, Object> metaData;

    /**
     * Constructs a new aggregator factory.
     *
     * @param name  The aggregation name
     * @param type  The aggregation type
     */
    public AggregatorFactory(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Registers sub-factories with this factory. The sub-factory will be responsible for the creation of sub-aggregators under the
     * aggregator created by this factory.
     *
     * @param subFactories  The sub-factories
     * @return  this factory (fluent interface)
     */
    public AggregatorFactory subFactories(AggregatorFactories subFactories) {
        this.factories = subFactories;
        this.factories.setParent(this);
        return this;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly configured)
     */
    public final void validate() {
        doValidate();
        factories.validate();
    }

    /**
     * @return  The parent factory if one exists (will always return {@code null} for top level aggregator factories).
     */
    public AggregatorFactory parent() {
        return parent;
    }

    protected abstract Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsOnly0, Map<String, Object> metaData) throws IOException;

    /**
     * Creates the aggregator
     *
     * @param context               The aggregation context
     * @param parent                The parent aggregator (if this is a top level factory, the parent will be {@code null})
     * @param collectsOnly0         If true then the created aggregator will only be collected with <tt>0</tt> as a bucket ordinal.
     *                              Some factories can take advantage of this in order to return more optimized implementations.
     *
     * @return                      The created aggregator
     */
    public final Aggregator create(AggregationContext context, Aggregator parent, boolean collectsOnly0) throws IOException {
        Aggregator aggregator = createInternal(context, parent, collectsOnly0, this.metaData);
        return aggregator;
    }

    public void doValidate() {
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    /**
     * Utility method. Given an {@link AggregatorFactory} that creates {@link Aggregator}s that only know how
     * to collect bucket <tt>0</tt>, this returns an aggregator that can collect any bucket.
     */
    protected static Aggregator asMultiBucketAggregator(final AggregatorFactory factory, final AggregationContext context, Aggregator parent) throws IOException {
        final Aggregator first = factory.create(context, parent, true);
        return new Aggregator(first.name(), AggregatorFactories.EMPTY, first.context(), first.parent(), first.getMetaData()) {

            ObjectArray<Aggregator> aggregators;
            LeafReaderContext readerContext;

            {
                aggregators = bigArrays.newObjectArray(1);
                aggregators.set(0, first);
            }

            @Override
            public boolean shouldCollect() {
                return first.shouldCollect();
            }

            @Override
            protected void doPreCollection() throws IOException {
                for (long i = 0; i < aggregators.size(); ++i) {
                    final Aggregator aggregator = aggregators.get(i);
                    if (aggregator != null) {
                        aggregator.preCollection();
                    }
                }
            }

            @Override
            protected void doPostCollection() throws IOException {
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
                    aggregator = factory.create(context, parent, true);
                    aggregator.preCollection();
                    aggregator.setNextReader(readerContext);
                    aggregators.set(owningBucketOrdinal, aggregator);
                }
                aggregator.collect(doc, 0);
            }

            @Override
            public void setNextReader(LeafReaderContext context) throws IOException {
                this.readerContext = context;
                for (long i = 0; i < aggregators.size(); ++i) {
                    final Aggregator aggregator = aggregators.get(i);
                    if (aggregator != null) {
                        aggregator.setNextReader(context);
                    }
                }
            }

            @Override
            public InternalAggregation buildAggregation(long owningBucketOrdinal) {
                throw new ElasticsearchIllegalStateException("Invalid context - aggregation must use addResults() to collect child results");
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return first.buildEmptyAggregation();
            }

            @Override
            public void doClose() {
                Releasables.close(aggregators);
            }

            @Override
            public void gatherAnalysis(BucketAnalysisCollector results, long owningBucketOrdinal) throws IOException {
                // The bucket ordinal may be out of range in case of eg. a terms/filter/terms where
                // the filter matches no document in the highest buckets of the first terms agg
                if (owningBucketOrdinal >= aggregators.size() || aggregators.get(owningBucketOrdinal) == null) {
                    results.add(first.buildEmptyAggregation());
                } else {
                    aggregators.get(owningBucketOrdinal).gatherAnalysis(results,0);
                }
            }
        };
    }

}
