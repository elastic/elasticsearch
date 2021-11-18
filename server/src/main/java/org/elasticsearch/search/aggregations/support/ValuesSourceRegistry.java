/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@link ValuesSourceRegistry} holds the mapping from {@link ValuesSourceType}s to functions for building aggregation components.  DO NOT
 * directly instantiate this class, instead get an already-configured copy from {@link SearchExecutionContext#getValuesSourceRegistry()},
 * or (in the case of some test scenarios only) directly from {@link SearchModule#getValuesSourceRegistry()}
 *
 */
public class ValuesSourceRegistry {

    public static final class RegistryKey<T> {
        private final String name;
        private final Class<T> supplierType;

        public RegistryKey(String name, Class<T> supplierType) {
            this.name = Objects.requireNonNull(name);
            this.supplierType = Objects.requireNonNull(supplierType);
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegistryKey<?> that = (RegistryKey<?>) o;
            return name.equals(that.name) && supplierType.equals(that.supplierType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, supplierType);
        }
    }

    @SuppressWarnings("rawtypes")
    public static final RegistryKey UNREGISTERED_KEY = new RegistryKey<>("unregistered", RegistryKey.class);

    public static class Builder {
        private final AggregationUsageService.Builder usageServiceBuilder;
        private Map<RegistryKey<?>, List<Map.Entry<ValuesSourceType, ?>>> aggregatorRegistry = new HashMap<>();
        private Map<String, ValuesSourceType> valueTypeLookup = new HashMap<>();
        private Set<ValuesSourceType> duplicateRegistrationCheck = new HashSet<>();

        public Builder() {
            this.usageServiceBuilder = new AggregationUsageService.Builder();
        }

        /**
         * Register a ValuesSource to Aggregator mapping. This method registers mappings that only apply to a
         * single {@link ValuesSourceType}
         * @param registryKey The name of the family of aggregations paired with the expected component supplier type for this
         *                    family of aggregations.  Generally, the aggregation builder is expected to define a constant for use as the
         *                    registryKey
         * @param valuesSourceType The ValuesSourceType this mapping applies to.
         * @param aggregatorSupplier An Aggregation-specific specialization of ComponentSupplier which will construct the mapped aggregator
         * @param registerUsage Flag to indicate if this aggregation values source combo should be added to the usage registry.
         *                      Aggregations that set this to false should register with the usage registry through some other path.
         */
        public <T> void register(
            RegistryKey<T> registryKey,
            ValuesSourceType valuesSourceType,
            T aggregatorSupplier,
            boolean registerUsage
        ) {
            if (aggregatorRegistry.containsKey(registryKey) == false) {
                aggregatorRegistry.put(registryKey, new ArrayList<>());
            }
            aggregatorRegistry.get(registryKey).add(new AbstractMap.SimpleEntry<>(valuesSourceType, aggregatorSupplier));
            if (registerUsage) {
                registerUsage(registryKey.getName(), valuesSourceType);
            }
        }

        /**
         * Register a ValuesSource to Aggregator mapping. This version provides a convenience method for mappings that apply to a
         * known list of {@link ValuesSourceType}
         * @param registryKey The name of the family of aggregations paired with the expected component supplier type for this
         *                    family of aggregations.  Generally, the aggregation builder is expected to define a constant for use as the
         *                    registryKey
         * @param valuesSourceTypes The ValuesSourceTypes this mapping applies to.
         * @param aggregatorSupplier An Aggregation-specific specialization of ComponentSupplier which will construct the mapped aggregator
         * @param registerUsage Flag to indicate if this aggregation values source combo should be added to the usage registry.
         *                      Aggregations that set this to false should register with the usage registry through some other path.
         */
        public <T> void register(
            RegistryKey<T> registryKey,
            List<ValuesSourceType> valuesSourceTypes,
            T aggregatorSupplier,
            boolean registerUsage
        ) {
            for (ValuesSourceType valuesSourceType : valuesSourceTypes) {
                register(registryKey, valuesSourceType, aggregatorSupplier, registerUsage);
            }
        }

        public void registerUsage(String aggregationName, ValuesSourceType valuesSourceType) {
            usageServiceBuilder.registerAggregationUsage(aggregationName, valuesSourceType.typeName());
        }

        public void registerUsage(String aggregationName) {
            usageServiceBuilder.registerAggregationUsage(aggregationName);
        }

        public void registerValuesSourceType(ValuesSourceType valuesSourceType, Collection<String> alternateNames) {
            if (duplicateRegistrationCheck.contains(valuesSourceType)) {
                throw new IllegalStateException("Duplicate registration of ValuesSourceType [" + valuesSourceType.typeName() + "]");
            }
            valueTypeLookup.put(valuesSourceType.typeName(), valuesSourceType);
            for (String name : alternateNames) {
                valueTypeLookup.put(name, valuesSourceType);
            }
            duplicateRegistrationCheck.add(valuesSourceType);
        }

        public ValuesSourceRegistry build() {
            return new ValuesSourceRegistry(aggregatorRegistry, usageServiceBuilder.build(), valueTypeLookup);
        }
    }

    private static Map<RegistryKey<?>, Map<ValuesSourceType, ?>> copyMap(
        Map<RegistryKey<?>, List<Map.Entry<ValuesSourceType, ?>>> mutableMap
    ) {
        /*
         Make an immutable copy of our input map. Since this is write once, read many, we'll spend a bit of extra time to shape this
         into a Map.of(), which is more read optimized than just using a hash map.
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map.Entry<RegistryKey<?>, Map<ValuesSourceType, ?>>[] copiedEntries = new Map.Entry[mutableMap.size()];
        int i = 0;
        for (Map.Entry<RegistryKey<?>, List<Map.Entry<ValuesSourceType, ?>>> entry : mutableMap.entrySet()) {
            RegistryKey<?> topKey = entry.getKey();
            List<Map.Entry<ValuesSourceType, ?>> values = entry.getValue();
            @SuppressWarnings({ "rawtypes", "unchecked" })
            Map.Entry<RegistryKey<?>, Map<ValuesSourceType, ?>> newEntry = Map.entry(
                topKey,
                Map.ofEntries(values.toArray(new Map.Entry[0]))
            );
            copiedEntries[i++] = newEntry;
        }
        return Map.ofEntries(copiedEntries);
    }

    /** Maps Aggregation names to (ValuesSourceType, Supplier) pairs, keyed by ValuesSourceType */
    private final AggregationUsageService usageService;
    private Map<RegistryKey<?>, Map<ValuesSourceType, ?>> aggregatorRegistry;
    private Map<String, ValuesSourceType> valueTypeLookup;

    public ValuesSourceRegistry(
        Map<RegistryKey<?>, List<Map.Entry<ValuesSourceType, ?>>> aggregatorRegistry,
        AggregationUsageService usageService,
        Map<String, ValuesSourceType> valueTypeLookup) {
        this.aggregatorRegistry = copyMap(aggregatorRegistry);
        this.usageService = usageService;
        // TODO: Make an immutable copy blah blah blah
        this.valueTypeLookup = valueTypeLookup;
    }

    public boolean isRegistered(RegistryKey<?> registryKey) {
        return aggregatorRegistry.containsKey(registryKey);
    }

    public <T> T getAggregator(RegistryKey<T> registryKey, ValuesSourceConfig valuesSourceConfig) {
        if (registryKey != null && aggregatorRegistry.containsKey(registryKey)) {
            @SuppressWarnings("unchecked")
            T supplier = (T) aggregatorRegistry.get(registryKey).get(valuesSourceConfig.valueSourceType());
            if (supplier == null) {
                throw new IllegalArgumentException(
                    valuesSourceConfig.getDescription() + " is not supported for aggregation [" + registryKey.getName() + "]"
                );
            }
            return supplier;
        }
        throw new AggregationExecutionException("Unregistered Aggregation [" + registryKey.getName() + "]");
    }

    public AggregationUsageService getUsageService() {
        return usageService;
    }

    public ValuesSourceType resolveTypeHint(String typeHint) {
        return typeHint != null ? ValueType.lenientParse(typeHint).getValuesSourceType() : null;
    }
}
