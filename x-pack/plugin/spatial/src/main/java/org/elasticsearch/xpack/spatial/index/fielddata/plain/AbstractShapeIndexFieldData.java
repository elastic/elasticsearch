/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.search.SortField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

public abstract class AbstractShapeIndexFieldData implements IndexShapeFieldData {
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<ShapeValues> toScriptFieldFactory;

    AbstractShapeIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<ShapeValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        throw new IllegalArgumentException("can't sort on geo_shape field without using specific sorting feature, like geo_distance");
    }

    public static class GeoBuilder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final ToScriptFieldFactory<ShapeValues> toScriptFieldFactory;

        public GeoBuilder(String name, ValuesSourceType valuesSourceType, ToScriptFieldFactory<ShapeValues> toScriptFieldFactory) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            // ignore breaker
            return new LatLonShapeIndexFieldData(name, valuesSourceType, toScriptFieldFactory);
        }
    }
}
