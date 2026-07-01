/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node registry for components that must be shared between plugins before Guice wiring is available, i.e. during
 * {@link Plugin#createComponents}. A plugin that builds a component another plugin needs during its own {@code createComponents} can
 * {@link #register} it here; the consuming plugin retrieves it with {@link #get}.
 *
 * <p>Exactly one instance exists per node (see {@link Plugin.PluginServices#sharedComponents()}), constructed fresh for every node,
 * so a component registered here is never visible to another node, even when multiple nodes run in the same JVM, as in tests
 * (including multiple nodes built concurrently, e.g. by tests that start several clusters in parallel).
 *
 * <p>Load ordering still applies: a plugin that calls {@link #get} for a type must be loaded after the plugin that {@link #register}s
 * it (today, via {@code extendedPlugins}).
 *
 * <p>Plugins wired through Guice should prefer having a dependency injected (published as a {@code PluginComponentBinding} from
 * {@code createComponents}) over using this registry; it exists specifically for the createComponents-time gap where Guice is not
 * yet available.
 */
public final class SharedComponentRegistry {

    private final Map<Class<?>, Object> components = new ConcurrentHashMap<>();

    /**
     * Registers a component under the given type.
     *
     * @throws IllegalStateException if a component is already registered under {@code type}
     */
    public <T> void register(Class<T> type, T component) {
        Object existing = components.putIfAbsent(type, Objects.requireNonNull(component));
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
        Object component = components.get(type);
        if (component == null) {
            throw new IllegalStateException(type.getSimpleName() + " is not registered yet");
        }
        return type.cast(component);
    }
}
