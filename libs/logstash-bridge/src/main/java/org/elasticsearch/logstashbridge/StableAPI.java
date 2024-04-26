/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@code StableAPI} is the stable form of an Elasticsearch API, and can produce comparable instances
 * from the actual API that they mirror. As part of the LogstashBridge project, these classes are relied
 * upon by the "Elastic Integration Filter Plugin" for Logstash and their external shapes mut not change
 * without coordination with the maintainers of that project.
 *
 * @param <T> the actual type of the Elasticsearch API being mirrored
 */
public interface StableAPI<T> {
    T unwrap();

    static <T> T unwrapNullable(final StableAPI<T> nullableStableAPI) {
        if (Objects.isNull(nullableStableAPI)) {
            return null;
        }
        return nullableStableAPI.unwrap();
    }

    static <K, T> Map<K, T> unwrap(final Map<K, ? extends StableAPI<T>> bridgeMap) {
        return bridgeMap.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().unwrap()));
    }

    static <K, T, B extends StableAPI<T>> Map<K, B> wrap(final Map<K, T> rawMap, final Function<T, B> wrapFunction) {
        return rawMap.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> wrapFunction.apply(e.getValue())));
    }

    static <T, B extends StableAPI<T>> B wrap(final T delegate, final Function<T, B> wrapFunction) {
        if (Objects.isNull(delegate)) {
            return null;
        }
        return wrapFunction.apply(delegate);
    }

    abstract class Proxy<T> implements StableAPI<T> {
        protected final T delegate;

        protected Proxy(final T delegate) {
            this.delegate = delegate;
        }

        @Override
        public T unwrap() {
            return delegate;
        }
    }
}
