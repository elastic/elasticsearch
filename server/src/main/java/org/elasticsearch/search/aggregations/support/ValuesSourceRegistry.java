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
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * {@link ValuesSourceRegistry} holds the mapping from {@link ValuesSourceType}s to {@link AggregatorSupplier}s.  DO NOT directly
 * instantiate this class, instead get an already-configured copy from {@link QueryShardContext#getValuesSourceRegistry()}, or (in the case
 * of some test scenarios only) directly from {@link SearchModule#getValuesSourceRegistry()}
 *
 */
public class ValuesSourceRegistry {
    // Maps Aggregation names to (ValuesSourceType, Supplier) pairs, keyed by ValuesSourceType
    private Map<String, List<Map.Entry<Predicate<ValuesSourceType>, AggregatorSupplier>>> aggregatorRegistry = Map.of();

    /**
     * Register a ValuesSource to Aggregator mapping.
     *
     * Threading behavior notes: This call is both synchronized and expensive. It copies the entire existing mapping structure each
     * time it is invoked.  We expect that register will be called a small number of times during startup only (as plugins are being
     * registered) and we can tolerate the cost at that time.  Once all plugins are registered, we should never need to call register
     * again.  Comparatively, we expect to do many reads from the registry data structures, and those reads may be interleaved on
     * different worker threads.  Thus we want to optimize the read case to be thread safe and fast, which the immutable
     * collections do well.  Using immutable collections requires a copy on write mechanic, thus the somewhat non-intuitive
     * implementation of this method.
     * @param aggregationName The name of the family of aggregations, typically found via ValuesSourceAggregationBuilder.getType()
     * @param appliesTo A predicate which accepts the resolved {@link ValuesSourceType} and decides if the given aggregator can be applied
     *                  to that type.
     * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
     */
    public synchronized void register(String aggregationName, Predicate<ValuesSourceType> appliesTo,
                                      AggregatorSupplier aggregatorSupplier) {
        AbstractMap.SimpleEntry[] mappings;
        if (aggregatorRegistry.containsKey(aggregationName)) {
            List currentMappings = aggregatorRegistry.get(aggregationName);
            mappings = (AbstractMap.SimpleEntry[]) currentMappings.toArray(new AbstractMap.SimpleEntry[currentMappings.size() + 1]);
        } else {
            mappings = new AbstractMap.SimpleEntry[1];
        }
        mappings[mappings.length - 1] = new AbstractMap.SimpleEntry<>(appliesTo, aggregatorSupplier);
        aggregatorRegistry = copyAndAdd(aggregatorRegistry,new AbstractMap.SimpleEntry<>(aggregationName, List.of(mappings)));
    }

    /**
     * Register a ValuesSource to Aggregator mapping.  This version provides a convenience method for mappings that only apply to a single
     * {@link ValuesSourceType}, to allow passing in the type and auto-wrapping it in a predicate
     *  @param aggregationName The name of the family of aggregations, typically found via ValuesSourceAggregationBuilder.getType()
     * @param valuesSourceType The ValuesSourceType this mapping applies to.
     * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
     *                           from the aggregation standard set of parameters
     */
    public void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier) {
        register(aggregationName, (candidate) -> valuesSourceType.equals(candidate), aggregatorSupplier);
    }

    /**
     * Register a ValuesSource to Aggregator mapping.  This version provides a convenience method for mappings that only apply to a known
     * list of {@link ValuesSourceType}, to allow passing in the type and auto-wrapping it in a predicate
     *  @param aggregationName The name of the family of aggregations, typically found via ValuesSourceAggregationBuilder.getType()
     * @param valuesSourceTypes The ValuesSourceTypes this mapping applies to.
     * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
     *                           from the aggregation standard set of parameters
     */
    public void register(String aggregationName, List<ValuesSourceType> valuesSourceTypes, AggregatorSupplier aggregatorSupplier) {
        register(aggregationName, (candidate) -> {
            for (ValuesSourceType valuesSourceType : valuesSourceTypes) {
                if (valuesSourceType.equals(candidate)) {
                    return true;
                }
            }
            return false;
        }, aggregatorSupplier);
    }

    private AggregatorSupplier findMatchingSuppier(ValuesSourceType valuesSourceType,
                                                   List<Map.Entry<Predicate<ValuesSourceType>, AggregatorSupplier>> supportedTypes) {
        for (Map.Entry<Predicate<ValuesSourceType>, AggregatorSupplier> candidate : supportedTypes) {
            if (candidate.getKey().test(valuesSourceType)) {
                return candidate.getValue();
            }
        }
        return null;
    }

    public AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName) {
        if (aggregationName != null && aggregatorRegistry.containsKey(aggregationName)) {
            AggregatorSupplier supplier = findMatchingSuppier(valuesSourceType, aggregatorRegistry.get(aggregationName));
            if (supplier == null) {
                throw new AggregationExecutionException("ValuesSource type " + valuesSourceType.toString() +
                    " is not supported for aggregation" + aggregationName);
            }
            return supplier;
        }
        throw  new AggregationExecutionException("Unregistered Aggregation [" + aggregationName + "]");
    }

    public ValuesSourceType getValuesSourceType(MappedFieldType fieldType, String aggregationName,
                                                // TODO: the following arguments are only needed for the legacy case
                                                IndexFieldData indexFieldData,
                                                ValueType valueType, Script script,
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
            } else if (indexFieldData instanceof IndexHistogramFieldData) {
                return CoreValuesSourceType.HISTOGRAM;
            } else {
                if (valueType == null) {
                    return defaultValuesSourceType;
                } else {
                    return valueType.getValuesSourceType();
                }
            }
        }
    }

    private static <K, V> Map copyAndAdd(Map<K, V>  source, Map.Entry<K, V>  newValue) {
        Map.Entry[] entries;
        if (source.containsKey(newValue.getKey())) {
            // Replace with new value
            entries = new Map.Entry[source.size()];
            int i = 0;
            for (Map.Entry entry : source.entrySet()) {
                if (entry.getKey() == newValue.getKey()) {
                    entries[i] = newValue;
                } else {
                    entries[i] = entry;
                }
                i++;
            }
        } else {
            entries = source.entrySet().toArray(new Map.Entry[source.size() + 1]);
            entries[entries.length - 1] = newValue;
        }
        return Map.ofEntries(entries);
    }

}
