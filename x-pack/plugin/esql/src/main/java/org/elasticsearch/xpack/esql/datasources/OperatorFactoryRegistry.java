/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Registry for source operator factories.
 *
 * <p>This registry provides a single entry point for creating source operator factories.
 * It supports two modes:
 * <ol>
 *   <li><b>Plugin factories</b>: Custom factories registered by plugins for complex
 *       datasources (Iceberg, Delta Lake) that need specialized logic.</li>
 *   <li><b>Generic factory</b>: Falls back to {@link AsyncExternalSourceOperatorFactory}
 *       for simple formats (CSV, JSON, Parquet) using the StorageProvider and FormatReader
 *       abstractions.</li>
 * </ol>
 *
 * <p>The lookup order is:
 * <ol>
 *   <li>Check if a plugin has registered a custom factory for the source type</li>
 *   <li>If not, use the generic async factory with storage and format registries</li>
 * </ol>
 *
 * <p>Note: Method names follow the project convention of omitting the "get" prefix.
 */
public class OperatorFactoryRegistry {

    private final Map<String, SourceOperatorFactoryProvider> pluginFactories;
    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final Executor executor;
    private final Settings settings;

    public OperatorFactoryRegistry(
        Map<String, SourceOperatorFactoryProvider> pluginFactories,
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        Executor executor,
        Settings settings
    ) {
        if (storageRegistry == null) {
            throw new IllegalArgumentException("storageRegistry cannot be null");
        }
        if (formatRegistry == null) {
            throw new IllegalArgumentException("formatRegistry cannot be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        this.pluginFactories = pluginFactories != null ? Map.copyOf(pluginFactories) : Map.of();
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.executor = executor;
        this.settings = settings != null ? settings : Settings.EMPTY;
    }

    public SourceOperator.SourceOperatorFactory factory(SourceOperatorContext context) {
        String sourceType = context.sourceType();

        // 1. Plugin provides custom factory? Use it.
        if (sourceType != null && pluginFactories.containsKey(sourceType)) {
            return pluginFactories.get(sourceType).create(context);
        }

        // 2. Otherwise: generic async factory (handles CSV, JSON, Parquet, etc.)
        StoragePath path = context.path();

        // Resolve the storage provider.
        // When the context carries per-query config (e.g. endpoint, credentials from WITH clause),
        // create a fresh provider with that config. This is scheme-agnostic: S3, HTTP, LOCAL, or any
        // future backend — the config is forwarded to the SPI factory's create(settings, config).
        // Schemes that don't override that method simply ignore the config via the default delegation.
        // TODO: Per-query providers created here via createProvider() are not closed after the operator
        // finishes. Factories don't have a close lifecycle, and the provider must stay alive while
        // StorageObject streams are active. Full lifecycle tracking would require plumbing a Releasable
        // through the driver framework. For now this is acceptable because SPI-created providers
        // (e.g. S3StorageProvider) typically wrap pooled/shared clients.
        Map<String, Object> config = context.config();
        StorageProvider storage;
        if (config != null && config.isEmpty() == false) {
            storage = storageRegistry.createProvider(path.scheme(), settings, config);
        } else {
            storage = storageRegistry.provider(path);
        }

        FormatReader format = formatRegistry.byExtension(path.objectName());

        if (storage == null) {
            throw new IllegalArgumentException("No storage provider registered for scheme: " + path.scheme());
        }
        if (format == null) {
            throw new IllegalArgumentException("No format reader registered for file: " + path.objectName());
        }

        return new AsyncExternalSourceOperatorFactory(
            storage,
            format,
            path,
            context.attributes(),
            context.batchSize(),
            context.maxBufferSize(),
            executor,
            context.fileSet()
        );
    }

    public boolean hasPluginFactory(String sourceType) {
        return sourceType != null && pluginFactories.containsKey(sourceType);
    }

    public StorageProviderRegistry storageRegistry() {
        return storageRegistry;
    }

    public FormatReaderRegistry formatRegistry() {
        return formatRegistry;
    }

    public Executor executor() {
        return executor;
    }

}
