/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.xpack.core.inference.SerializableStats;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A map to provide tracking incrementing statistics.
 *
 * @param <Input> The input to derive the keys and values for the map
 * @param <Value> The type of the values stored in the map
 */
public class StatsMap<Input, Value extends Stats> {

    private final ConcurrentMap<String, Value> stats = new ConcurrentHashMap<>();
    private final Function<Input, String> keyCreator;
    private final Function<Input, Value> valueCreator;

    /**
     * @param keyCreator a function for creating a key in the map based on the input provided
     * @param valueCreator a function for creating a value in the map based on the input provided
     */
    public StatsMap(Function<Input, String> keyCreator, Function<Input, Value> valueCreator) {
        this.keyCreator = Objects.requireNonNull(keyCreator);
        this.valueCreator = Objects.requireNonNull(valueCreator);
    }

    /**
     * Increment the counter for a particular value in a thread safe manner.
     * @param input the input to derive the appropriate key in the map
     */
    public void increment(Input input) {
        var value = stats.computeIfAbsent(keyCreator.apply(input), key -> valueCreator.apply(input));
        value.increment();
    }

    /**
     * Build a map that can be serialized. This takes a snapshot of the current state. Any concurrent calls to increment may or may not
     * be represented in the resulting serializable map.
     * @return a map that is more easily serializable
     */
    public Map<String, SerializableStats> toSerializableMap() {
        return stats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toSerializableForm()));
    }
}
