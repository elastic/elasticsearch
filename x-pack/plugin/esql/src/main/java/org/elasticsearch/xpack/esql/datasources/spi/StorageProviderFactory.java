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
     * <p>
     * <b>Override target:</b> implementations must override {@link #createTrackingConsumedKeys(Settings, Map)},
     * NOT this method. The default {@code create(Settings, Map)} delegates through the tracking variant,
     * so an override here alone would be silently bypassed by every caller. The tracking variant is the
     * single configuration entry point for the SPI.
     */
    default StorageProvider create(Settings settings, Map<String, Object> config) {
        return createTrackingConsumedKeys(settings, config).value();
    }

    /**
     * Per-query overload paired with the keys consumed from {@code config}.
     * Default ignores config and returns an empty consumed-keys set.
     * <p>
     * <b>This is the canonical override target</b> — overriding only {@link #create(Settings, Map)} would
     * be silently bypassed because {@code create(Settings, Map)}'s default delegates to this method.
     * The consumed-keys set is required by {@link ConfigKeyValidator} for unknown-key rejection at
     * planning time.
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
                StorageProvider provider = resolved.value() != null ? providerCtor.apply(resolved.value()) : defaultProvider.get();
                return new Configured<>(provider, resolved.consumedKeys());
            }
        };
    }
}
