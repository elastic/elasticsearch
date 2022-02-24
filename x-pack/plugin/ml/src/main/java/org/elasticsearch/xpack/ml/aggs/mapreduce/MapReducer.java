/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.ToXContent;

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
 */
public interface MapReducer extends NamedWriteable, ToXContent {

    /**
     * Returns the name of the writable aggregation object, this must match the name of the aggregation builder
     *
     * The map reduce itself has a writable name, too. Don't mix them up.
     * The usecase: different versions of a map-reducer in 1 aggregation
     *
     * In theory you can also use 1 map-reducer in 2 different aggregations.
     */
    String getAggregationWritableName();

    /**
     * Definition of the mapper that gets executed locally on every shard
     *
     * TODO: assumes only 1 "flat" input, what about multi inputs?
     *
     * @param keyValues A stream of keys and values, while a value is a list of values.
     */
    void map(Stream<Tuple<String, List<Object>>> keyValues);

    /**
     * Definition of the reducer that gets results from every shard
     *
     * @param partitions individual map reduce instances which hold the data
     */
    void reduce(Stream<MapReducer> partitions);

    /**
     * Definition of the combiner that works as a local reducer, reducing partial data.
     *
     * @param partitions
     */
    default void combine(Stream<MapReducer> partitions) {
        reduce(partitions);
    }

    /**
     * Definition of code to execute before the mapper processes any input.
     */
    default void mapInit() {};

    /**
     * Definition of code to execute after the mapper processed all input.
     */
    default void mapFinalize() {};

    /**
     * Definition of code to execute before the reducer processes any input.
     */
    default void reduceInit() {};

    /**
     * Definition of code to execute after the reducer processed all input.
     */
    default void reduceFinalize() {};

    /**
     * Forwarded from {@link InternalAggregation}:
     *
     * Signal the framework if the {@linkplain InternalAggregation#reduce(List, ReduceContext)} phase needs to be called
     * when there is only one {@linkplain InternalAggregation}.
     */
    default boolean mustReduceOnSingleInternalAgg() {
        return true;
    }
}
