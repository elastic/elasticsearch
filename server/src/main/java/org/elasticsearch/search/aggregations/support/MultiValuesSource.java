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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to encapsulate a set of ValuesSource objects labeled by field name
 */
public abstract class MultiValuesSource <VS extends ValuesSource> {
    protected Map<String, VS> values;

    public static class NumericMultiValuesSource extends MultiValuesSource<ValuesSource.Numeric> {
        public NumericMultiValuesSource(Map<String, ValuesSourceConfig> valuesSourceConfigs,
                                        QueryShardContext context) {
            values = new HashMap<>(valuesSourceConfigs.size());
            for (Map.Entry<String, ValuesSourceConfig> entry : valuesSourceConfigs.entrySet()) {
                final ValuesSource valuesSource = entry.getValue().toValuesSource();
                if (valuesSource instanceof ValuesSource.Numeric == false) {
                    throw new AggregationExecutionException("ValuesSource type " + valuesSource.toString() +
                        "is not supported for multi-valued aggregation");
                }
                values.put(entry.getKey(), (ValuesSource.Numeric) valuesSource);
            }
        }

        public SortedNumericDoubleValues getField(String fieldName, LeafReaderContext ctx) throws IOException {
            ValuesSource.Numeric value = values.get(fieldName);
            if (value == null) {
                throw new IllegalArgumentException("Could not find field name [" + fieldName + "] in multiValuesSource");
            }
            return value.doubleValues(ctx);
        }
    }

    public boolean needsScores() {
        return values.values().stream().anyMatch(ValuesSource::needsScores);
    }

    public boolean areValuesSourcesEmpty() {
        return values.values().stream().allMatch(Objects::isNull);
    }
}
