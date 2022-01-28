/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.NamedWriteable;
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
     * Definition of the mapper that gets executed locally on every shard
     *
     * TODO: assumes only 1 "flat" input, what about multi inputs?
     *
     * @param values A stream of values, while a value itself is a list of values.
     */
    void map(Stream<List<Object>> values);

    /**
     * Definition of the reducer that gets results from every shard
     *
     * @param partitions individual map reduce instances which hold the data
     */
    void reduce(Stream<MapReducer> partitions);

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
