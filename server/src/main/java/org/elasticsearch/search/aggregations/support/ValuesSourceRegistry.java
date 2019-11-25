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
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/*
This is a _very_ crude prototype for the ValuesSourceRegistry which basically hard-codes everything.  The intent is to define the API
for aggregations using the registry to resolve aggregators.
 */
public enum ValuesSourceRegistry {
    INSTANCE {
        Map<String, Map<ValuesSourceType, AggregatorSupplier>> aggregatorRegistry = new HashMap<>();
        // We use a List of Entries here to approximate an ordered map
        Map<String, List<AbstractMap.SimpleEntry<BiFunction<MappedFieldType, IndexFieldData, Boolean>, ValuesSourceType>>> resolverRegistry
            = new HashMap<>();

        @Override
        public void register(String aggregationName, ValuesSourceType valuesSourceType,AggregatorSupplier aggregatorSupplier,
                             BiFunction<MappedFieldType, IndexFieldData, Boolean> resolveValuesSourceType) {
            if (resolverRegistry.containsKey(aggregationName) == false) {
                resolverRegistry.put(aggregationName, new ArrayList<>());
            }
            List<AbstractMap.SimpleEntry<BiFunction<MappedFieldType, IndexFieldData, Boolean>, ValuesSourceType>> resolverList
                = resolverRegistry.get(aggregationName);
            resolverList.add(new AbstractMap.SimpleEntry<>(resolveValuesSourceType, valuesSourceType));

            if (aggregatorRegistry.containsKey(aggregationName) == false) {
                aggregatorRegistry.put(aggregationName, new HashMap<>());
            }
            Map<ValuesSourceType, AggregatorSupplier> innerMap = aggregatorRegistry.get(aggregationName);
            if (innerMap.containsKey(valuesSourceType)) {
                throw new IllegalStateException("Attempted to register already registered pair [" + aggregationName + ", "
                    + valuesSourceType.toString() + "]");
            }
            innerMap.put(valuesSourceType, aggregatorSupplier);
        }

        @Override
        public AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName) {
            if (aggregatorRegistry.containsKey(aggregationName)) {
                Map<ValuesSourceType, AggregatorSupplier> innerMap = aggregatorRegistry.get(aggregationName);
                if (innerMap.containsKey(valuesSourceType)) {
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
            if (resolverRegistry.containsKey(aggregationName)) {
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
}
