/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexPointFieldData;
import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

abstract class AbstractPointIndexFieldData<T extends MultiPointValues<? extends SpatialPoint>> implements IndexPointFieldData<T> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<T> toScriptFieldFactory;

    protected AbstractPointIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<T> toScriptFieldFactory
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
        throw new IllegalArgumentException(
            "can't sort on geo_point or point field without using specific sorting feature, like geo_distance"
        );
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException(
            "can't sort on geo_point or point field without using specific sorting feature, like geo_distance"
        );
    }
}
