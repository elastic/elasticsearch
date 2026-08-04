/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.util.CachedSupplier;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A per-node registry for sharing component instances between plugins during node startup.
 *
 * <p>Plugins exchange components here during {@link Plugin#createComponents}, before Guice is available.
 * A producing plugin calls {@link #register} to publish a component by type.
 * A consuming plugin calls {@link #get} to retrieve it.
 *
 * <p>Each node gets its own instance, so components registered here are never visible to other nodes.
 *
 * <p>Plugins initialize in load order. A consumer must load after its producer, typically enforced via
 * {@code extendedPlugins}. When load order cannot be guaranteed, use {@link #getProvider}.
 */
public final class SharedComponentRegistry {

    private final Map<Class<?>, Object> components = new ConcurrentHashMap<>();

    /**
     * Registers a component under the given type.
     *
     * @throws IllegalStateException if a component is already registered under {@code type}
     */
    public <T> void register(Class<T> type, T component) {
        var existing = components.putIfAbsent(
            Objects.requireNonNull(type, "type must not be null"),
            Objects.requireNonNull(component, "component must not be null")
        );
        if (existing != null) {
            throw new IllegalStateException(type.getSimpleName() + " is already registered");
        }
    }

    /**
     * Returns the component registered under the given type.
     *
     * @throws IllegalStateException if no component is registered under {@code type} yet
     */
    public <T> T get(Class<T> type) {
        var component = components.get(type);
        if (component == null) {
            throw new IllegalStateException(type.getSimpleName() + " is not registered yet");
        }
        return type.cast(component);
    }

    /**
     * Returns the component registered under the given type, or {@code defaultValue} if none is registered.
     */
    public <T> T getOrDefault(Class<T> type, T defaultValue) {
        var component = components.get(Objects.requireNonNull(type, "type must not be null"));
        return component != null ? type.cast(component) : defaultValue;
    }

    /**
     * Returns a provider for the component of the given type.
     *
     * <p>Unlike {@link #get}, this does not require the component to be registered at call time. The returned
     * {@link Supplier} defers the lookup until its {@code get()} method is first invoked, which is useful when
     * the registration order across plugins cannot be guaranteed. The result is memoized after the first successful
     * lookup.
     */
    public <T> Supplier<T> getProvider(Class<T> type) {
        Objects.requireNonNull(type, "type must not be null");
        return CachedSupplier.wrap(() -> get(type));
    }
}
