/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@code StableBridgeAPI} is the stable bridge to an Elasticsearch API, and can produce instances
 * from the actual API that they mirror. As part of the LogstashBridge project, these classes are relied
 * upon by the "Elastic Integration Filter Plugin" for Logstash and their external shapes must not change
 * without coordination with the maintainers of that project.
 *
 * @param <INTERNAL> the actual type of the Elasticsearch API being mirrored
 */
public interface StableBridgeAPI<INTERNAL> {
    INTERNAL toInternal();

    static <T> T toInternalNullable(final StableBridgeAPI<T> nullableStableBridgeAPI) {
        if (Objects.isNull(nullableStableBridgeAPI)) {
            return null;
        }
        return nullableStableBridgeAPI.toInternal();
    }

    static <K, T> Map<K, T> toInternal(final Map<K, ? extends StableBridgeAPI<T>> bridgeMap) {
        return bridgeMap.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().toInternal()));
    }

    static <K, T, B extends StableBridgeAPI<T>> Map<K, B> fromInternal(final Map<K, T> rawMap, final Function<T, B> externalizor) {
        return rawMap.entrySet()
                     .stream()
                     .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> externalizor.apply(e.getValue())));
    }

    static <T, B extends StableBridgeAPI<T>> B fromInternal(final T delegate, final Function<T, B> externalizor) {
        if (Objects.isNull(delegate)) {
            return null;
        }
        return externalizor.apply(delegate);
    }

    /**
     * An {@code ProxyInternal<INTERNAL>} is an implementation of {@code StableBridgeAPI<INTERNAL>} that
     * proxies calls to a delegate that is an actual {@code INTERNAL}.
     *
     * @param <INTERNAL>
     */
    abstract class ProxyInternal<INTERNAL> implements StableBridgeAPI<INTERNAL> {
        protected final INTERNAL internalDelegate;

        protected ProxyInternal(final INTERNAL internalDelegate) {
            this.internalDelegate = internalDelegate;
        }

        @Override
        public INTERNAL toInternal() {
            return internalDelegate;
        }
    }
}
