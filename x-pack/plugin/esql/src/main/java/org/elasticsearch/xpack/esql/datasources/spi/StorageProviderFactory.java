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
 * <p>
 * Two abstract methods: {@link #create(Settings)} for cluster-settings-only construction, and
 * {@link #createTrackingConsumedKeys(Settings, Map)} for per-query construction paired with the
 * keys this factory claims from the configuration map. Both must be implemented explicitly so a
 * factory cannot silently drop per-query configuration keys.
 * <p>
 * Two static helpers cover the common shapes:
 * <ul>
 *   <li>{@link #noConfigKeys(Supplier)} — for factories that recognise no per-query keys today.
 *       Forwards to the supplier in both methods; any non-empty config map is rejected by the
 *       generic validator at planning time as "unknown options".
 *   <li>{@link #of(Supplier, Function, Function)} — for factories that resolve a typed
 *       {@code Configuration} from the map and construct a provider from it.
 * </ul>
 */
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
     * <p>
     * <b>Required override.</b> Every factory must explicitly declare which per-query keys it
     * claims, even if the answer is "none" (use {@link #noConfigKeys(Supplier)} for that case).
     * The consumed-keys set is required by {@link ConfigKeyValidator} for unknown-key rejection
     * at planning time; a silent default would let typo'd configurations slip through.
     */
    Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config);

    /**
     * Builds a {@link StorageProviderFactory} for providers that have no per-query configuration
     * keys. Both {@link #create(Settings)} and {@link #createTrackingConsumedKeys(Settings, Map)}
     * delegate to {@code provider}; the tracking variant returns {@link Configured#empty} so
     * {@link ConfigKeyValidator} rejects any non-empty config map at planning time.
     */
    static <P extends StorageProvider> StorageProviderFactory noConfigKeys(Supplier<P> provider) {
        return new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return provider.get();
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                return Configured.empty(provider.get());
            }
        };
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
