/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
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
 * @param <MapContext> context to be used for collecting
 * @param <MapFinalContext> context after all data of one partition has been mapped
 * @param <ReduceContext> context to be used for reducing data
 * @param <Result> the result object that holds the result of this map-reducer
 *
 */
public abstract class AbstractItemSetMapReducer<
    MapContext extends Closeable,
    MapFinalContext extends Writeable,
    ReduceContext extends Closeable,
    Result extends ToXContent & Writeable> implements NamedWriteable {

    private final String aggregationName;
    private final String mapReducerName;

    protected AbstractItemSetMapReducer(String aggregationName, String mapReducerName) {
        this.aggregationName = aggregationName;
        this.mapReducerName = mapReducerName;
    }

    /**
     * Definition of code to execute before the mapper processes any input.
     *
     * This is mandatory to create a context object for storing data.
     */
    protected abstract MapContext mapInit(BigArrays bigArrays);

    /**
     * Definition of the mapper that gets executed locally on every shard
     *
     * @param keyValues A stream of keys and values, while a value is a list of values.
     * @param mapContext context object for mapping
     */
    protected abstract MapContext map(Stream<Tuple<Field, List<Object>>> keyValues, MapContext mapContext);

    /**
     * Definition of code to execute(optional) after the mapper processed all input.
     *
     */
    protected abstract MapFinalContext mapFinalize(MapContext mapContext);

    /**
     * Definition of code to execute before the reducer processes any input.
     *
     * This is mandatory to create a result object that holds reduced data.
     */
    protected abstract ReduceContext reduceInit(BigArrays bigArrays);

    /**
     * Definition of the reducer that gets results from every shard
     *
     * @param partitions individual map reduce context instances which hold the data
     * @param reduceContext the result object create by doReduceInit
     * @param isCanceledSupplier supplier to check whether the request has been canceled
     */
    protected abstract ReduceContext reduce(
        Stream<MapFinalContext> partitions,
        ReduceContext reduceContext,
        Supplier<Boolean> isCanceledSupplier
    );

    /**
     * Definition of the combiner that works as a local reducer, reducing partial data.
     *
     * This can be optionally overwritten, otherwise it calls doReduce
     *
     * @param partitions individual map reduce context instances which hold the data
     * @param reduceContext the result object created by doReduceInit
     * @param isCanceledSupplier supplier to check whether the request has been canceled
     *
     */
    protected abstract MapFinalContext combine(
        Stream<MapFinalContext> partitions,
        ReduceContext reduceContext,
        Supplier<Boolean> isCanceledSupplier
    );

    /**
     * Definition of code to execute after the reducer processed all input.
     *
     * @param reduceContext the result object returned from doReduce
     * @param fields list of fields from the input
     * @param isCanceledSupplier supplier to check whether the request has been canceled
     * @throws IOException
     */
    protected abstract Result reduceFinalize(ReduceContext reduceContext, List<Field> fields, Supplier<Boolean> isCanceledSupplier)
        throws IOException;

    /**
     * Definition of code to execute if sampling has been applied.
     *
     * You must overwrite this if the results of this map-reducer contains absolute doc counts in any way.
     *
     * @param samplingContext the sampling context
     * @param result the mapReduceResult to be adjusted for sampling
     */
    protected Result finalizeSampling(SamplingContext samplingContext, Result result) {
        return result;
    }

    /**
     * Definition of code to read map-reduce context from a map-reduce operation from a stream.
     *
     * This must be implemented, for writing the context must implement `Writeable`
     *
     * @param in the input stream
     * @param bigArrays instance of BigArrays to use
     * @return a MapReduceContext
     * @throws IOException
     */
    protected abstract MapFinalContext readMapReduceContext(StreamInput in, BigArrays bigArrays) throws IOException;

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
    protected abstract Result readResult(StreamInput in, BigArrays bigArrays) throws IOException;

    /**
     * Extension point to add further information for `profile:true`
     *
     * @param add callback to add a string/object pair as debug info
     */
    protected void collectDebugInfo(BiConsumer<String, Object> add) {}

    /**
     * Forwarded from {@link InternalAggregation}:
     *
     * Signal the framework if the {@linkplain InternalAggregation#reduce(List, AggregationReduceContext)} phase needs to be called
     * when there is only one {@linkplain InternalAggregation}.
     */
    final boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public final String getWriteableName() {
        return aggregationName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(mapReducerName);
    }

}
