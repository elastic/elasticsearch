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
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/*
This is a _very_ crude prototype for the ValuesSourceRegistry which basically hard-codes everything.  The intent is to define the API
for aggregations using the registry to resolve aggregators.
 */
public enum ValuesSourceRegistry {
    INSTANCE {
        Map<String, Map<ValuesSourceType, AggregatorSupplier>> aggregatorRegistry = Map.of();
        // We use a List of Entries here to approximate an ordered map
        Map<String, List<AbstractMap.SimpleEntry<BiFunction<MappedFieldType, IndexFieldData, Boolean>, ValuesSourceType>>> resolverRegistry
            = Map.of();

        /**
         * Threading behavior notes: This call is both synchronized and expensive. It copies the entire existing mapping structure each
         * time it is invoked.  We expect that register will be called a small number of times during startup only (as plugins are being
         * registered) and we can tolerate the cost at that time.  Once all plugins are registered, we should never need to call register
         * again.  Comparatively, we expect to do many reads from the registry data structures, and those reads may be interleaved on
         * different worker threads.  Thus we want to optimize the read case to be thread safe and fast, which the immutable
         * collections do well.  Using immutable collections requires a copy on write mechanic, thus the somewhat non-intuitive
         * implementation of this method.
         *
         * @param aggregationName The name of the family of aggregations, typically found via ValuesSourceAggregationBuilder.getType()
         * @param valuesSourceType The ValuesSourceType this mapping applies to.
         * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
         *                           from the aggregation standard set of parameters
         * @param resolveValuesSourceType A predicate operating on MappedFieldType and IndexFieldData instances which decides if the mapped
         */
        @Override
        public synchronized void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier,
                             BiFunction<MappedFieldType, IndexFieldData, Boolean> resolveValuesSourceType) {
            // Aggregator registry block - do this first in case we need to throw on duplicate registration
            Map<ValuesSourceType, AggregatorSupplier> innerMap;
            if (aggregatorRegistry.containsKey(aggregationName)) {
                if (aggregatorRegistry.get(aggregationName).containsKey(valuesSourceType)) {
                    throw new IllegalStateException("Attempted to register already registered pair [" + aggregationName + ", "
                        + valuesSourceType.toString() + "]");
                }
                innerMap = copyAndAdd(aggregatorRegistry.get(aggregationName),
                    new AbstractMap.SimpleEntry<>(valuesSourceType, aggregatorSupplier));
            } else {
                innerMap = Map.of(valuesSourceType, aggregatorSupplier);
            }
            aggregatorRegistry = copyAndAdd(aggregatorRegistry, new AbstractMap.SimpleEntry<>(aggregationName, innerMap));

            // Resolver registry block
            AbstractMap.SimpleEntry[] mappings;
            if (resolverRegistry.containsKey(aggregationName)) {
                List currentMappings = resolverRegistry.get(aggregationName);
                mappings = (AbstractMap.SimpleEntry[]) currentMappings.toArray(new AbstractMap.SimpleEntry[currentMappings.size() + 1]);
            } else {
                mappings = new AbstractMap.SimpleEntry[1];
            }
            mappings[mappings.length - 1] = new AbstractMap.SimpleEntry<>(resolveValuesSourceType, valuesSourceType);
            resolverRegistry = copyAndAdd(resolverRegistry,new AbstractMap.SimpleEntry<>(aggregationName, List.of(mappings)));
        }

        @Override
        public AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName) {
            if (aggregationName != null && aggregatorRegistry.containsKey(aggregationName)) {
                Map<ValuesSourceType, AggregatorSupplier> innerMap = aggregatorRegistry.get(aggregationName);
                if (valuesSourceType != null && innerMap.containsKey(valuesSourceType)) {
                    return innerMap.get(valuesSourceType);
                }
            }
            // TODO: Error message should list valid ValuesSource types
            throw new AggregationExecutionException("ValuesSource type " + valuesSourceType.toString() +
                " is not supported for aggregation" + aggregationName);
        }

        @Override
        public ValuesSourceType getValuesSourceType(MappedFieldType fieldType, IndexFieldData indexFieldData, String aggregationName,
                                                    ValueType valueType) {
            if (aggregationName != null && resolverRegistry.containsKey(aggregationName)) {
                List<AbstractMap.SimpleEntry<BiFunction<MappedFieldType, IndexFieldData, Boolean>, ValuesSourceType>> resolverList
                    = resolverRegistry.get(aggregationName);
                for (AbstractMap.SimpleEntry<BiFunction<MappedFieldType, IndexFieldData, Boolean>, ValuesSourceType> entry : resolverList) {
                    BiFunction<MappedFieldType, IndexFieldData, Boolean> matcher = entry.getKey();
                    if (matcher.apply(fieldType, indexFieldData)) {
                        return entry.getValue();
                    }
                }
                // TODO: Error message should list valid field types; not sure fieldType.toString() is the best choice.
                throw new IllegalArgumentException("Field type " + fieldType.toString() + " is not supported for aggregation "
                    + aggregationName);
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
                        return CoreValuesSourceType.BYTES;
                    } else {
                        return valueType.getValuesSourceType();
                    }
                }
            }
        }
    };

    /**
     * Register a ValuesSource to Aggregator mapping.
     *
     * @param aggregationName The name of the family of aggregations, typically found via ValuesSourceAggregationBuilder.getType()
     * @param valuesSourceType The ValuesSourceType this mapping applies to.
     * @param aggregatorSupplier An Aggregation-specific specialization of AggregatorSupplier which will construct the mapped aggregator
     *                           from the aggregation standard set of parameters
     * @param resolveValuesSourceType A predicate operating on MappedFieldType and IndexFieldData instances which decides if the mapped
     *                                ValuesSourceType can be applied to the given field.
     */
    public abstract void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier,
                                  BiFunction<MappedFieldType, IndexFieldData, Boolean> resolveValuesSourceType);

    public abstract AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName);
    // TODO: ValueType argument is only needed for legacy logic
    public abstract ValuesSourceType getValuesSourceType(MappedFieldType fieldType, IndexFieldData indexFieldData, String aggregationName,
                                                         ValueType valueType);

    public static ValuesSourceRegistry getInstance() {return INSTANCE;}

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
