/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.usage.UsageService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link ValuesSourceRegistry} holds the mapping from {@link ValuesSourceType}s to {@link AggregatorSupplier}s.  DO NOT directly
 * instantiate this class, instead get an already-configured copy from {@link QueryShardContext#getValuesSourceRegistry()}, or (in the case
 * of some test scenarios only) directly from {@link SearchModule#getValuesSourceRegistry()}
 */
public class ValuesSourceRegistry {
    /**
     * Base class for the aggregation registration.
     * <p>
     * TODO: we can get rid of this entire hierarchy and shrink it to a single class or even Tuple when we get rid of registerAny
     */
    public abstract static class RegistryEntry {

        private final AggregatorSupplier supplier;

        /**
         * @param supplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         *                 from the aggregation standard set of parameters
         */
        protected RegistryEntry(AggregatorSupplier supplier) {
            this.supplier = supplier;
        }

        /**
         * Returns true if this aggregation can be applied to the given value source type, false otherwise
         */
        protected abstract boolean appliesTo(ValuesSourceType type);

        /**
         * Increments usage counter for the given aggregetion
         */
        protected abstract void inc(ValuesSourceType type);

        /**
         * Registers the aggregation usage with the usage service
         */
        protected abstract void registerUsage(String aggName, UsageService usageService);

    }

    private static class AnyRegistryEntry extends RegistryEntry {
        private final AtomicLong stats;

        private AnyRegistryEntry(AggregatorSupplier supplier) {
            super(supplier);
            this.stats = new AtomicLong();
        }

        @Override
        protected void inc(ValuesSourceType type) {
            stats.incrementAndGet();
        }

        @Override
        protected void registerUsage(String aggName, UsageService usageService) {
            usageService.addAggregationUsage(aggName, "*", stats);
        }

        @Override
        protected boolean appliesTo(ValuesSourceType type) {
            return true;
        }
    }


    private static class SingleRegistryEntry extends RegistryEntry {
        private final AtomicLong stats;
        private final ValuesSourceType type;

        private SingleRegistryEntry(AggregatorSupplier supplier, ValuesSourceType type) {
            super(supplier);
            this.stats = new AtomicLong();
            this.type = type;
        }

        @Override
        protected void inc(ValuesSourceType type) {
            stats.incrementAndGet();
        }

        @Override
        protected void registerUsage(String aggName, UsageService usageService) {
            usageService.addAggregationUsage(aggName, type.value(), stats);
        }

        @Override
        protected boolean appliesTo(ValuesSourceType type) {
            return this.type == type;
        }
    }

    private static class MultiRegistryEntry extends RegistryEntry {
        private final Map<ValuesSourceType, AtomicLong> stats;
        private final List<ValuesSourceType> valuesSourceTypes;

        private MultiRegistryEntry(AggregatorSupplier supplier, List<ValuesSourceType> valuesSourceTypes) {
            super(supplier);
            this.stats = new HashMap<>();
            this.valuesSourceTypes = valuesSourceTypes;
            for (ValuesSourceType valuesSourceType : valuesSourceTypes) {
                AtomicLong stat = new AtomicLong();
                stats.put(valuesSourceType, stat);
            }
        }

        @Override
        protected void inc(ValuesSourceType type) {
            stats.get(type).incrementAndGet();
        }

        @Override
        protected boolean appliesTo(ValuesSourceType type) {
            for (ValuesSourceType valuesSourceType : valuesSourceTypes) {
                if (valuesSourceType.equals(type)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void registerUsage(String aggName, UsageService usageService) {
            for (ValuesSourceType valuesSourceType : valuesSourceTypes) {
                usageService.addAggregationUsage(aggName, valuesSourceType.value(), stats.get(valuesSourceType));
            }
        }
    }

    public static class Builder {
        private final Map<String, List<RegistryEntry>> aggregatorRegistry = new HashMap<>();

        /**
         * Register a ValuesSource to Aggregator mapping.
         *
         * @param aggregationName    The name of the family of aggregations, typically found via
         *                           {@link ValuesSourceAggregationBuilder#getType()}
         * @param registryEntry An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         */
        private void register(String aggregationName, RegistryEntry registryEntry) {
            if (aggregatorRegistry.containsKey(aggregationName) == false) {
                aggregatorRegistry.put(aggregationName, new ArrayList<>());
            }
            aggregatorRegistry.get(aggregationName).add(registryEntry);
        }

        /**
         * Register a ValuesSource to Aggregator mapping.  This version provides a convenience method for mappings that only apply to a
         * single {@link ValuesSourceType}, to allow passing in the type and auto-wrapping it in a predicate
         *
         * @param aggregationName    The name of the family of aggregations, typically found via
         *                           {@link ValuesSourceAggregationBuilder#getType()}
         * @param valuesSourceType   The ValuesSourceType this mapping applies to.
         * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         *                           from the aggregation standard set of parameters
         */
        public void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier) {
            register(aggregationName, new SingleRegistryEntry(aggregatorSupplier, valuesSourceType));
        }

        /**
         * Register a ValuesSource to Aggregator mapping.  This version provides a convenience method for mappings that only apply to a
         * known list of {@link ValuesSourceType}, to allow passing in the type and auto-wrapping it in a predicate
         *
         * @param aggregationName    The name of the family of aggregations, typically found via
         *                           {@link ValuesSourceAggregationBuilder#getType()}
         * @param valuesSourceTypes  The ValuesSourceTypes this mapping applies to.
         * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         *                           from the aggregation standard set of parameters
         */
        public void register(String aggregationName, List<ValuesSourceType> valuesSourceTypes, AggregatorSupplier aggregatorSupplier) {
            register(aggregationName, new MultiRegistryEntry(aggregatorSupplier, valuesSourceTypes));
        }

        /**
         * Register an aggregator that applies to any values source type.  This is a convenience method for aggregations that do not care at
         * all about the types of their inputs.  Aggregations using this version of registration should not make any other registrations, as
         * the aggregator registered using this function will be applied in all cases.
         *
         * @param aggregationName    The name of the family of aggregations, typically found via
         *                           {@link ValuesSourceAggregationBuilder#getType()}
         * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         *                           from the aggregation standard set of parameters.
         */
        public void registerAny(String aggregationName, AggregatorSupplier aggregatorSupplier) {
            register(aggregationName, new AnyRegistryEntry(aggregatorSupplier));
        }

        public ValuesSourceRegistry build() {
            return new ValuesSourceRegistry(aggregatorRegistry);
        }
    }

    /**
     * Maps Aggregation names to RegistryEntry
     */
    private final Map<String, List<RegistryEntry>> aggregatorRegistry;

    public ValuesSourceRegistry(Map<String, List<RegistryEntry>> aggregatorRegistry) {
        /*
         Make an immutable copy of our input map.  Since this is write once, read many, we'll spend a bit of extra time to shape this
         into a Map.of(), which is more read optimized than just using a hash map.
         */
        @SuppressWarnings("unchecked") Map.Entry<String, List<RegistryEntry>>[] copiedEntries = new Map.Entry[aggregatorRegistry.size()];
        int i = 0;
        for (Map.Entry<String, List<RegistryEntry>> entry : aggregatorRegistry.entrySet()) {
            String aggName = entry.getKey();
            List<RegistryEntry> values = entry.getValue();
            Map.Entry<String, List<RegistryEntry>> newEntry = Map.entry(aggName, List.of(values.toArray(new RegistryEntry[0])));
            copiedEntries[i++] = newEntry;
        }
        this.aggregatorRegistry = Map.ofEntries(copiedEntries);
    }

    private RegistryEntry findMatchingSuppier(ValuesSourceType valuesSourceType, List<RegistryEntry> supportedTypes) {
        for (RegistryEntry candidate : supportedTypes) {
            if (candidate.appliesTo(valuesSourceType)) {
                return candidate;
            }
        }
        return null;
    }

    public AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName) {
        if (aggregationName != null && aggregatorRegistry.containsKey(aggregationName)) {
            RegistryEntry entry = findMatchingSuppier(valuesSourceType, aggregatorRegistry.get(aggregationName));
            if (entry == null) {
                throw new AggregationExecutionException("ValuesSource type " + valuesSourceType.toString() +
                    " is not supported for aggregation" + aggregationName);
            }
            return entry.supplier;
        }
        throw new AggregationExecutionException("Unregistered Aggregation [" + aggregationName + "]");
    }

    public ValuesSourceType getValuesSourceType(MappedFieldType fieldType, String aggregationName,
                                                // TODO: the following arguments are only needed for the legacy case
                                                IndexFieldData<?> indexFieldData,
                                                ValueType valueType,
                                                ValuesSourceType defaultValuesSourceType) {
        if (aggregationName != null && aggregatorRegistry.containsKey(aggregationName)) {
            // This will throw if the field doesn't support values sources, although really we probably threw much earlier in that case
            ValuesSourceType valuesSourceType = fieldType.getValuesSourceType();
            if (aggregatorRegistry.get(aggregationName) != null
                && findMatchingSuppier(valuesSourceType, aggregatorRegistry.get(aggregationName)) != null) {
                return valuesSourceType;
            }
            String fieldDescription = fieldType.typeName() + "(" + fieldType.toString() + ")";
            throw new IllegalArgumentException("Field [" + fieldType.name() + "] of type [" + fieldDescription +
                "] is not supported for aggregation [" + aggregationName + "]");
        } else {
            // TODO: Legacy resolve logic; remove this after converting all aggregations to the new system
            if (indexFieldData instanceof IndexNumericFieldData) {
                return CoreValuesSourceType.NUMERIC;
            } else if (indexFieldData instanceof IndexGeoPointFieldData) {
                return CoreValuesSourceType.GEOPOINT;
            } else if (fieldType instanceof RangeFieldMapper.RangeFieldType) {
                return CoreValuesSourceType.RANGE;
            } else {
                if (valueType == null) {
                    return defaultValuesSourceType;
                } else {
                    return valueType.getValuesSourceType();
                }
            }
        }
    }

    public void incUsage(String aggregationName, ValuesSourceType valuesSourceType) {
        if (aggregatorRegistry.get(aggregationName) != null) {
            RegistryEntry entry = findMatchingSuppier(valuesSourceType, aggregatorRegistry.get(aggregationName));
            if (entry != null) {
                entry.inc(valuesSourceType);
            }
        }
    }

    public void registerUsage(UsageService usageService) {
        for (Map.Entry<String, List<RegistryEntry>> mapEntries : aggregatorRegistry.entrySet()) {
            for (RegistryEntry entry : mapEntries.getValue()) {
                entry.registerUsage(mapEntries.getKey(), usageService);
            }
        }
    }
}
