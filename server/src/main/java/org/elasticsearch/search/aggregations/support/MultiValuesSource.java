/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
        public NumericMultiValuesSource(Map<String, ValuesSourceConfig> valuesSourceConfigs) {
            values = new HashMap<>(valuesSourceConfigs.size());
            for (Map.Entry<String, ValuesSourceConfig> entry : valuesSourceConfigs.entrySet()) {
                final ValuesSource valuesSource = entry.getValue().getValuesSource();
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
