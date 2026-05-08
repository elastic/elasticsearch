/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Factory for creating {@link StorageProvider} instances.
 * This functional interface allows data source plugins to provide
 * storage provider implementations without exposing implementation details.
 */
@FunctionalInterface
public interface StorageProviderFactory {

    StorageProvider create(Settings settings);

    /**
     * Per-query overload that takes a configuration map.
     * Default delegates to {@link #createTrackingConsumedKeys(Settings, Map)} and discards the consumed-keys set;
     * use this overload when the caller does not need to validate against the consumed keys.
     */
    default StorageProvider create(Settings settings, Map<String, Object> config) {
        return createTrackingConsumedKeys(settings, config).value();
    }

    /**
     * Per-query overload paired with the keys consumed from {@code config}.
     * Default ignores config and returns an empty consumed-keys set.
     * Implementations that read configuration from the map should override this method
     * (not {@link #create(Settings, Map)}); the simple overload is a default convenience.
     */
    default Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
        return Configured.empty(create(settings));
    }

    /**
     * Builds a {@link StorageProviderFactory} for the common pattern: the no-config path constructs a
     * provider from a supplier (e.g. {@code () -> new S3StorageProvider(null)}); the per-query path
     * resolves a configuration via {@code configFactory}, threads its consumed-keys set through, and
     * constructs a provider from the resolved configuration via {@code providerCtor}. Replaces the
     * anonymous-class boilerplate that otherwise repeats per scheme.
     */
    static <C, P extends StorageProvider> StorageProviderFactory of(
        Supplier<P> defaultProvider,
        Function<Map<String, Object>, Configured<C>> configFactory,
        Function<C, P> providerCtor
    ) {
        return new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return defaultProvider.get();
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                if (config == null || config.isEmpty()) {
                    return Configured.empty(defaultProvider.get());
                }
                Configured<C> resolved = configFactory.apply(config);
                return new Configured<>(providerCtor.apply(resolved.value()), resolved.consumedKeys());
            }
        };
    }
}
