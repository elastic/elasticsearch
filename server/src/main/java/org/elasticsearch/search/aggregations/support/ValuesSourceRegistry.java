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

import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.HashMap;
import java.util.Map;

/*
This is a _very_ crude prototype for the ValuesSourceRegistry which basically hard-codes everything.  The intent is to define the API
for aggregations using the registry to resolve aggregators.
 */
public enum ValuesSourceRegistry {
    INSTANCE {
        Map<String, Map<ValuesSourceType, AggregatorSupplier>> aggregatorRegistry = new HashMap<>();

        @Override
        public void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier) {
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
            throw new AggregationExecutionException("ValuesSource type " + valuesSourceType.toString() + " is not supported for aggregation" +
                aggregationName);
        }
    };

    public abstract void register(String aggregationName, ValuesSourceType valuesSourceType, AggregatorSupplier aggregatorSupplier);
    public abstract AggregatorSupplier getAggregator(ValuesSourceType valuesSourceType, String aggregationName);

    public static ValuesSourceRegistry getInstance() {return INSTANCE;}
}
