/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public final class InternalItemSetMapReduceAggregation<
    MapContext extends Closeable,
    MapFinalContext extends Writeable,
    ReduceContext extends Closeable,
    Result extends ToXContent & Writeable> extends InternalAggregation {

    private final AbstractItemSetMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer;
    private final BigArrays bigArraysForMapReduce;
    private final List<Field> fields;
    private final boolean profiling;

    private MapFinalContext mapFinalContext = null;
    private Result mapReduceResult = null;

    InternalItemSetMapReduceAggregation(
        String name,
        Map<String, Object> metadata,
        AbstractItemSetMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer,
        MapFinalContext mapFinalContext,
        Result mapReduceResult,
        List<Field> fields,
        boolean profiling
    ) {
        super(name, metadata);
        this.mapReducer = Objects.requireNonNull(mapReducer);
        this.mapFinalContext = mapFinalContext;
        this.mapReduceResult = mapReduceResult;
        this.fields = Objects.requireNonNull(fields);
        this.profiling = profiling;

        // we use the `NON_RECYCLING_INSTANCE` here, which has no circuit breaker attached
        // However circuit breaking works different in this place:
        // {@link QueryPhaseResultConsumer} reserves the serialized size of this object plus 50% extra
        // using the non-breaking and non-recycling instance is ok under the assumption that the real memory
        // usage is not more than 1.5 * the serialized size
        this.bigArraysForMapReduce = BigArrays.NON_RECYCLING_INSTANCE;
    }

    public InternalItemSetMapReduceAggregation(
        StreamInput in,
        Writeable.Reader<AbstractItemSetMapReducer<MapContext, MapFinalContext, ReduceContext, Result>> reader
    ) throws IOException {
        super(in);

        // we use the `NON_RECYCLING_INSTANCE` here, which has no circuit breaker attached
        // However circuit breaking works different in this place:
        // {@link QueryPhaseResultConsumer} reserves the serialized size of this object plus 50% extra
        // using the non-breaking and non-recycling instance is ok under the assumption that the real memory
        // usage is not more than 1.5 * the serialized size
        this.bigArraysForMapReduce = BigArrays.NON_RECYCLING_INSTANCE;

        this.mapReducer = reader.read(in);

        if (in.readBoolean()) {
            this.mapFinalContext = this.mapReducer.readMapReduceContext(in, bigArraysForMapReduce);
        }
        if (in.readBoolean()) {
            this.mapReduceResult = this.mapReducer.readResult(in, bigArraysForMapReduce);
        }

        this.fields = in.readList(Field::new);
        this.profiling = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return mapReducer.getWriteableName();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        mapReducer.writeTo(out);
        out.writeOptionalWriteable(mapFinalContext);
        out.writeOptionalWriteable(mapReduceResult);
        out.writeList(fields);
        out.writeBoolean(profiling);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext aggReduceContext) {

        Stream<MapFinalContext> contexts = aggregations.stream().map(agg -> {
            assert agg.getClass().equals(InternalItemSetMapReduceAggregation.class);
            @SuppressWarnings("unchecked")
            MapFinalContext context = ((InternalItemSetMapReduceAggregation<MapContext, MapFinalContext, ReduceContext, Result>) agg)
                .getMapFinalContext();
            return context;
        }).filter(c -> c != null);

        if (aggReduceContext.isFinalReduce()) {
            // we can use the reduce context big arrays, because we finalize here
            try (ReduceContext reduceContext = mapReducer.reduceInit(aggReduceContext.bigArrays())) {
                mapReducer.reduce(contexts, reduceContext, aggReduceContext.isCanceled());
                mapReduceResult = mapReducer.reduceFinalize(reduceContext, fields, aggReduceContext.isCanceled());
            } catch (IOException e) {
                throw new AggregationExecutionException("Final reduction failed", e);
            }

            return new InternalItemSetMapReduceAggregation<>(name, metadata, mapReducer, null, mapReduceResult, fields, profiling);
        }
        // else: combine
        // can't use the bigarray from the agg reduce context, because we don't finalize it here
        ReduceContext newMapReduceContext = mapReducer.reduceInit(bigArraysForMapReduce);
        MapFinalContext newMapFinalContext = mapReducer.combine(contexts, newMapReduceContext, aggReduceContext.isCanceled());

        return new InternalItemSetMapReduceAggregation<>(name, metadata, mapReducer, newMapFinalContext, null, fields, profiling);
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
        if (mapReduceResult != null) {
            return mapReduceResult.toXContent(
                builder,
                new DelegatingMapParams(Map.of(SearchProfileResults.PROFILE_FIELD, String.valueOf(profiling)), params)
            );
        }
        return builder;
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        mapReduceResult = mapReducer.finalizeSampling(samplingContext, mapReduceResult);
        return this;
    }

    MapFinalContext getMapFinalContext() {
        return mapFinalContext;
    }

    // testing only
    public Result getMapReduceResult() {
        return mapReduceResult;
    }
}
