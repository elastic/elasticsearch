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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregatorFactory extends ToXContentToBytes implements NamedWriteable<AggregatorFactory> {

    protected String name;
    protected String type;
    protected AggregatorFactory parent;
    protected AggregatorFactories factories = AggregatorFactories.EMPTY;
    protected Map<String, Object> metaData;
    private AggregationContext context;

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
     * Initializes this factory with the given {@link AggregationContext} ready
     * to create {@link Aggregator}s
     */
    public final void init(AggregationContext context) {
        this.context = context;
        doInit(context);
        this.factories.init(context);
    }

    /**
     * Allows the {@link AggregatorFactory} to initialize any state prior to
     * using it to create {@link Aggregator}s.
     * 
     * @param context
     *            the {@link AggregationContext} to use during initialization.
     */
    protected void doInit(AggregationContext context) {
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

    public String name() {
        return name;
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

    protected abstract Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

    /**
     * Creates the aggregator
     *
     * @param parent                The parent aggregator (if this is a top level factory, the parent will be {@code null})
     * @param collectsFromSingleBucket  If true then the created aggregator will only be collected with <tt>0</tt> as a bucket ordinal.
     *                              Some factories can take advantage of this in order to return more optimized implementations.
     *
     * @return                      The created aggregator
     */
    public final Aggregator create(Aggregator parent, boolean collectsFromSingleBucket) throws IOException {
        return createInternal(context, parent, collectsFromSingleBucket, this.factories.createPipelineAggregators(), this.metaData);
    }

    public void doValidate() {
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    @Override
    public final AggregatorFactory readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        AggregatorFactory factory = doReadFrom(name, in);
        factory.factories = AggregatorFactories.EMPTY.readFrom(in);
        factory.factories.setParent(this);
        factory.metaData = in.readMap();
        return factory;
    }

    // NORELEASE make this abstract when agg refactor complete
    protected AggregatorFactory doReadFrom(String name, StreamInput in) throws IOException {
        return null;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        doWriteTo(out);
        factories.writeTo(out);
        out.writeMap(metaData);
    }

    // NORELEASE make this abstract when agg refactor complete
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        if (this.metaData != null) {
            builder.field("meta", this.metaData);
        }
        builder.field(type);
        internalXContent(builder, params);

        if (factories != null && factories.count() > 0) {
            builder.field("aggregations");
            factories.toXContent(builder, params);

        }

        return builder.endObject();
    }

    // NORELEASE make this method abstract when agg refactor complete
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getWriteableName() {
        return type;
    }

    /**
     * Utility method. Given an {@link AggregatorFactory} that creates {@link Aggregator}s that only know how
     * to collect bucket <tt>0</tt>, this returns an aggregator that can collect any bucket.
     */
    protected static Aggregator asMultiBucketAggregator(final AggregatorFactory factory,
            final AggregationContext context, final Aggregator parent) throws IOException {
        final Aggregator first = factory.create(parent, true);
        final BigArrays bigArrays = context.bigArrays();
        return new Aggregator() {

            ObjectArray<Aggregator> aggregators;
            ObjectArray<LeafBucketCollector> collectors;

            {
                context.searchContext().addReleasable(this, Lifetime.PHASE);
                aggregators = bigArrays.newObjectArray(1);
                aggregators.set(0, first);
                collectors = bigArrays.newObjectArray(1);
            }

            @Override
            public String name() {
                return first.name();
            }

            @Override
            public AggregationContext context() {
                return first.context();
            }

            @Override
            public Aggregator parent() {
                return first.parent();
            }

            @Override
            public boolean needsScores() {
                return first.needsScores();
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
                    Scorer scorer;

                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                        this.scorer = scorer;
                    }

                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        aggregators = bigArrays.grow(aggregators, bucket + 1);
                        collectors = bigArrays.grow(collectors, bucket + 1);

                        LeafBucketCollector collector = collectors.get(bucket);
                        if (collector == null) {
                            Aggregator aggregator = aggregators.get(bucket);
                            if (aggregator == null) {
                                aggregator = factory.create(parent, true);
                                aggregator.preCollection();
                                aggregators.set(bucket, aggregator);
                            }
                            collector = aggregator.getLeafCollector(ctx);
                            collector.setScorer(scorer);
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
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(factories, metaData, name, type, doHashCode());
    }

    // NORELEASE make this method abstract here when agg refactor complete (so
    // that subclasses are forced to implement it)
    protected int doHashCode() {
        throw new UnsupportedOperationException(
                "This method should be implemented by a sub-class and should not rely on this method. When agg re-factoring is complete this method will be made abstract.");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AggregatorFactory other = (AggregatorFactory) obj;
        if (!Objects.equals(name, other.name))
            return false;
        if (!Objects.equals(type, other.type))
            return false;
        if (!Objects.equals(metaData, other.metaData))
            return false;
        if (!Objects.equals(factories, other.factories))
            return false;
        return doEquals(obj);
    }

    // NORELEASE make this method abstract here when agg refactor complete (so
    // that subclasses are forced to implement it)
    protected boolean doEquals(Object obj) {
        throw new UnsupportedOperationException(
                "This method should be implemented by a sub-class and should not rely on this method. When agg re-factoring is complete this method will be made abstract.");
    }

}
