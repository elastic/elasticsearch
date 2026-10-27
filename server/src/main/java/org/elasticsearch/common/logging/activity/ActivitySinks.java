/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.List;

/**
 * The {@link ActivitySinkProvider}s loaded from plugins, bound once per node so that each transport action can pick out the
 * sinks for its own context type.
 */
public final class ActivitySinks {

    public static final ActivitySinks EMPTY = new ActivitySinks(List.of());

    private final List<ActivitySinkProvider<?>> providers;

    /**
     * @param loaded providers as returned by the plugin service loader, which can only be asked for the raw type
     */
    @SuppressWarnings("rawtypes")
    public ActivitySinks(List<? extends ActivitySinkProvider> loaded) {
        this.providers = loaded.stream().<ActivitySinkProvider<?>>map(p -> p).toList();
    }

    /**
     * The sinks of the providers that handle the given context type.
     */
    public <C extends ActivityCaptureContext> List<ActivitySink<C, ?>> forType(Class<C> contextType) {
        return providers.stream().filter(p -> p.contextType() == contextType).<ActivitySink<C, ?>>map(ActivitySinks::sinkOf).toList();
    }

    /**
     * The cast is safe because the provider was selected by comparing {@link ActivitySinkProvider#contextType()} with the
     * requested class, and a provider's sink only accepts contexts of that type.
     */
    @SuppressWarnings("unchecked")
    private static <C extends ActivityCaptureContext> ActivitySink<C, ?> sinkOf(ActivitySinkProvider<?> provider) {
        return (ActivitySink<C, ?>) provider.sink();
    }
}
