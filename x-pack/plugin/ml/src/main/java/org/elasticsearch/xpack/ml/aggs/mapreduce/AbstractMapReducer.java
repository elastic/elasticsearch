/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContent.DelegatingMapParams;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * A simple map-reduce style interface that makes it easier to implement aggregations in map-reduce style.
 *
 * The main concepts of a map-reduce framework:
 *
 * - mapper: takes single key/value pairs and consumes them
 * - reducer: takes the output individual consumed outputs and creates the final result
 * - combiner: a "local" reducer that takes an incomplete set of consumed outputs, usually
 *             used as optimization/compression before sending data over the wire.
 *
 * Apart from that the interface offers several hooks to run initialization and finalization.
 *
 * As aggregations are not exactly a map-reduce framework some other moving parts are required:
 *
 * - code to serialize/deserialize data for sending it over the wire
 *   - data is stored inside of the map-reducer instance but not separately
 * - result output as XContent
 * - named writable magic
 *
 * IMPORTANT: every map reducer must be added to {@link MapReduceNamedContentProvider}
 *
 * @param <MapReduceContext> a context object to be used for collecting and reducing the data of this mapreducer
 * @param <MapReduceResult> the result object that holds the result of reduce
 *
 */
public abstract class AbstractMapReducer<
    MR extends AbstractMapReducer<MR, MapReduceContext, MapReduceResult>,
    MapReduceContext extends Writeable,
    MapReduceResult extends ToXContent & Writeable> implements NamedWriteable {

    private final String aggregationName;

    protected AbstractMapReducer(String aggregationName) {
        this.aggregationName = aggregationName;
    }

    protected AbstractMapReducer(StreamInput in) throws IOException {
        this.aggregationName = in.readString();
    }

    /**
     * Returns the name of the aggregation object, this must match the name of the aggregation builder
     *
     * The map-reducer has a writable name, too. Don't mix them up.
     * The usecase: different versions of a map-reducer in 1 aggregation
     *
     * In theory you can also use 1 map-reducer in 2 different aggregations.
     */
    public String getAggregationName() {
        return aggregationName;
    }

    /**
     * Definition of code to execute before the mapper processes any input.
     *
     * This is mandatory to create a context object for storing data.
     */
    protected abstract MapReduceContext doMapInit(BigArrays bigArrays);

    /**
     * Definition of the mapper that gets executed locally on every shard
     *
     * @param keyValues A stream of keys and values, while a value is a list of values.
     */
    protected abstract MapReduceContext doMap(Stream<Tuple<String, List<Object>>> keyValues, MapReduceContext mapReduceContext);

    /**
     * Definition of code to execute(optional) after the mapper processed all input.
     *
     */
    protected MapReduceContext doMapFinalize(MapReduceContext mapReduceContext) {
        return mapReduceContext;
    };

    /**
     * Definition of code to execute before the reducer processes any input.
     *
     * This is mandatory to create a result object that holds reduced data.
     */
    protected abstract MapReduceContext doReduceInit(BigArrays bigArrays);

    /**
     * Definition of the reducer that gets results from every shard
     *
     * @param partitions individual map reduce context instances which hold the data
     * @param mapReduceContext the result object create by doReduceInit
     */
    protected abstract MapReduceContext doReduce(Stream<MapReduceContext> partitions, MapReduceContext mapReduceContext);

    /**
     * Definition of the combiner that works as a local reducer, reducing partial data.
     *
     * This can be optionally overwritten, otherwise it calls doReduce
     *
     * @param partitions individual map reduce context instances which hold the data
     * @param mapReduceContext the result object created by doReduceInit
     *
     */
    protected MapReduceContext doCombine(Stream<MapReduceContext> partitions, MapReduceContext mapReduceContext) {
        return doReduce(partitions, mapReduceContext);
    }

    /**
     * Definition of code to execute after the reducer processed all input.
     *
     * @param mapReduceContext the result object returned from doReduce
     * @throws IOException
     */
    protected abstract MapReduceResult doReduceFinalize(MapReduceContext mapReduceContext) throws IOException;

    /**
     * Definition of code to execute if sampling has been applied.
     *
     * You must overwrite this if the results of this map-reducer contains absolute doc counts in any way.
     *
     * @param samplingContext the sampling context
     * @param mapReduceResult the mapReduceResult to be adjusted for sampling
     */
    protected MapReduceResult doFinalizeSampling(SamplingContext samplingContext, MapReduceResult mapReduceResult) {
        return mapReduceResult;
    }

    /**
     * Definition of code to read results from a map operation from a stream.
     *
     * This must be implemented, for writing the context must implement `Writeable`
     *
     * @param in the input stream
     * @param bigArrays instance of BigArrays to use
     * @return a MapReduceContext
     * @throws IOException
     */
    protected abstract MapReduceContext doReadMapContext(StreamInput in, BigArrays bigArrays) throws IOException;

    /**
     * Forwarded from {@link InternalAggregation}:
     *
     * Signal the framework if the {@linkplain InternalAggregation#reduce(List, ReduceContext)} phase needs to be called
     * when there is only one {@linkplain InternalAggregation}.
     */
    boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    /**
     * Methods for MapReduceAggregator and InternalMapReduceAggregation.
     *
     * This code assumes that both execute these methods in the right order and therefore the objects are correct.
     *
     * (By design MapReduceAggregator and InternalMapReduceAggregation are generic and don't know the internal type)
     */

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aggregationName);
    }

    Writeable mapInit(BigArrays bigArrays) {
        return doMapInit(bigArrays);
    }

    Writeable map(Stream<Tuple<String, List<Object>>> keyValues, Writeable mapReduceContext) {
        @SuppressWarnings("unchecked")
        MapReduceContext context = (MapReduceContext) mapReduceContext;

        return doMap(keyValues, context);
    }

    Writeable mapFinalize(Writeable mapReduceContext) {
        @SuppressWarnings("unchecked")
        MapReduceContext context = (MapReduceContext) mapReduceContext;

        return doMapFinalize(context);
    };

    Writeable reduceInit(BigArrays bigArrays) {
        return doReduceInit(bigArrays);
    }

    Writeable reduce(Stream<Writeable> partitions, Writeable mapReduceContext) {
        @SuppressWarnings("unchecked")
        MapReduceContext context = (MapReduceContext) mapReduceContext;

        @SuppressWarnings("unchecked")
        Stream<MapReduceContext> p = (Stream<MapReduceContext>) partitions;

        return doReduce(p, context);
    }

    Writeable combine(Stream<Writeable> partitions, Writeable mapReduceContext) {
        @SuppressWarnings("unchecked")
        MapReduceContext context = (MapReduceContext) mapReduceContext;

        @SuppressWarnings("unchecked")
        Stream<MapReduceContext> p = (Stream<MapReduceContext>) partitions;

        return doCombine(p, context);
    }

    Writeable reduceFinalize(Writeable mapReduceContext) throws IOException {
        @SuppressWarnings("unchecked")
        MapReduceContext context = (MapReduceContext) mapReduceContext;

        return doReduceFinalize(context);
    };

    Writeable finalizeSampling(SamplingContext samplingContext, Writeable mapReduceResult) {
        @SuppressWarnings("unchecked")
        MapReduceResult result = (MapReduceResult) mapReduceResult;

        return doFinalizeSampling(samplingContext, result);
    }

    void writeContext(StreamOutput out, Writeable Writeable) throws IOException {
        Writeable.writeTo(out);
    }

    MapReduceContext readMapContext(StreamInput in, BigArrays bigArrays) throws IOException {
        // TODO: what if this isn't a map context but the result object?
        // it seems to dangerous to assume that deserialization only happens on partial reduced objects...
        return doReadMapContext(in, bigArrays);
    }

    // we know that MapReduceResult implements ToXContent
    @SuppressWarnings("unchecked")
    XContentBuilder toXContent(Writeable mapReduceResult, XContentBuilder builder, DelegatingMapParams delegatingMapParams)
        throws IOException {
        return ((MapReduceResult) mapReduceResult).toXContent(builder, delegatingMapParams);
    };

}
