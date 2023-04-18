/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class GeoLineMultiValuesSource extends MultiValuesSource<ValuesSource> {
    public GeoLineMultiValuesSource(Map<String, ValuesSourceConfig> valuesSourceConfigs) {
        values = Maps.newMapWithExpectedSize(valuesSourceConfigs.size());
        for (Map.Entry<String, ValuesSourceConfig> entry : valuesSourceConfigs.entrySet()) {
            final ValuesSource valuesSource = entry.getValue().getValuesSource();
            if (valuesSource instanceof ValuesSource.Numeric == false && valuesSource instanceof ValuesSource.GeoPoint == false) {
                throw new AggregationExecutionException(
                    "ValuesSource type " + valuesSource.toString() + "is not supported for multi-valued aggregation"
                );
            }
            values.put(entry.getKey(), valuesSource);
        }
    }

    private ValuesSource getField(String fieldName) {
        ValuesSource valuesSource = values.get(fieldName);
        if (valuesSource == null) {
            throw new IllegalArgumentException("Could not find field name [" + fieldName + "] in multiValuesSource");
        }
        return valuesSource;
    }

    public SortedNumericDoubleValues getNumericField(String fieldName, LeafReaderContext ctx) throws IOException {
        ValuesSource valuesSource = getField(fieldName);
        if (valuesSource instanceof ValuesSource.Numeric) {
            return ((ValuesSource.Numeric) valuesSource).doubleValues(ctx);
        }
        throw new IllegalArgumentException("field [" + fieldName + "] is not a numeric type");
    }

    public MultiGeoPointValues getGeoPointField(String fieldName, LeafReaderContext ctx) {
        ValuesSource valuesSource = getField(fieldName);
        if (valuesSource instanceof ValuesSource.GeoPoint) {
            return ((ValuesSource.GeoPoint) valuesSource).geoPointValues(ctx);
        }
        throw new IllegalArgumentException("field [" + fieldName + "] is not a geo_point type");
    }

}
