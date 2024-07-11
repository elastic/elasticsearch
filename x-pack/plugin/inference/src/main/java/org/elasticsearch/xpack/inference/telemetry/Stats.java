/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Stats<T, Input, Value extends Counter & Serializable<T>> {

    private final ConcurrentMap<String, Value> stats = new ConcurrentHashMap<>();
    private final Function<Input, String> keyCreator;
    private final Function<Input, Value> valueCreator;

    public Stats(Function<Input, String> keyCreator, Function<Input, Value> valueCreator) {
        this.keyCreator = Objects.requireNonNull(keyCreator);
        this.valueCreator = Objects.requireNonNull(valueCreator);
    }

    public void increment(Input input) {
        var value = stats.computeIfAbsent(keyCreator.apply(input), key -> valueCreator.apply(input));
        value.increment();
    }

    public Map<String, T> toSerializableMap() {
        return stats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().convert()));
    }
}
