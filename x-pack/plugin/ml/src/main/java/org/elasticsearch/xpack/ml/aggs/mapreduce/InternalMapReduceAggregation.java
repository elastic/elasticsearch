/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class InternalMapReduceAggregation extends InternalAggregation {

    private final AbstractMapReducer<?, ?, ?> mapReducer;
    private Writeable mapReduceContext;
    private final boolean profiling;

    InternalMapReduceAggregation(
        String name,
        Map<String, Object> metadata,
        AbstractMapReducer<?, ?, ?> mapReducer,
        Writeable mapReduceContext,
        boolean profiling
    ) {
        super(name, metadata);
        this.mapReducer = Objects.requireNonNull(mapReducer);
        this.mapReduceContext = Objects.requireNonNull(mapReduceContext);
        this.profiling = profiling;
    }

    public InternalMapReduceAggregation(StreamInput in) throws IOException {
        super(in);
        this.mapReducer = in.readNamedWriteable(AbstractMapReducer.class);

        // TODO: we should not use `NON_RECYCLING_INSTANCE` here, however we don't have access to AggregationContext yet
        this.mapReduceContext = this.mapReducer.readMapContext(in, BigArrays.NON_RECYCLING_INSTANCE);
        this.profiling = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return mapReducer.getAggregationName();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {

        out.writeNamedWriteable(mapReducer);
        mapReducer.writeContext(out, mapReduceContext);
        out.writeBoolean(profiling);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        // TODO: as long as we haven't clarified who is closing, we can't use big arrays from the context
        Writeable newMapReduceContext = mapReducer.reduceInit(BigArrays.NON_RECYCLING_INSTANCE);
        // MapReduceContext newMapReduceContext = mapReducer.reduceInit(reduceContext.bigArrays());

        // TODO: if we use the recycling big array instance and something fails here the internal aggregations aren't closed
        if (reduceContext.isFinalReduce()) {
            mapReducer.reduce(
                aggregations.stream().map(agg -> ((InternalMapReduceAggregation) agg).getMapReduceContext()),
                newMapReduceContext
            );
            try {
                newMapReduceContext = mapReducer.reduceFinalize(newMapReduceContext);
            } catch (IOException e) {
                throw new AggregationExecutionException("Final reduction failed", e);
            }
        } else {
            newMapReduceContext = mapReducer.combine(
                aggregations.stream().map(agg -> ((InternalMapReduceAggregation) agg).getMapReduceContext()),
                newMapReduceContext
            );
        }

        return new InternalMapReduceAggregation(name, metadata, mapReducer, newMapReduceContext, profiling);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return mapReducer.mustReduceOnSingleInternalAgg();
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return mapReducer.toXContent(
            mapReduceContext,
            builder,
            new DelegatingMapParams(Map.of(SearchProfileResults.PROFILE_FIELD, String.valueOf(profiling)), params)
        );
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        mapReduceContext = mapReducer.finalizeSampling(samplingContext, mapReduceContext);
        return this;
    }

    public Writeable getMapReduceContext() {
        return mapReduceContext;
    }

}
