/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;

import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A plugin that provides alternative engine implementations.
 */
public interface EnginePlugin {

    /**
     * When an index is created this method is invoked for each engine plugin. Engine plugins can inspect the index settings to determine
     * whether or not to provide an engine factory for the given index. A plugin that is not overriding the default engine should return
     * {@link Optional#empty()}. If multiple plugins return an engine factory for a given index the index will not be created and an
     * {@link IllegalStateException} will be thrown during index creation.
     *
     * @return an optional engine factory
     */
    Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings);

    /**
     * Register the metrics available from the plugin
     * @param registrator accepts a string, which is a unique key for the registration (used for xcontent, serialization) together with the
     *                    base metric holder
     */
    default void registerDirectoryMetrics(BiConsumer<String, PluggableDirectoryMetricsHolder<?>> registrator) {

    }
}
