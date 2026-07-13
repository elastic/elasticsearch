/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

/**
 * This class needs to be extended with a type specific implementation.
 * In particular the `load(context)` method needs to return an instance of `LeadShapeFieldData` that is specific to the type of
 * `ValueSourceType` that this is constructed with. For example, the `LatLonShapeIndexFieldData.load(context)` method will
 * create an instance of `LatLonShapeDVAtomicShapeFieldData`.
 */
public abstract class AbstractShapeIndexFieldData<T extends ShapeValues<?>> implements IndexShapeFieldData<T> {
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;
    protected final ToScriptFieldFactory<T> toScriptFieldFactory;

    AbstractShapeIndexFieldData(String fieldName, ValuesSourceType valuesSourceType, ToScriptFieldFactory<T> toScriptFieldFactory) {
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
    public LeafShapeFieldData<T> loadDirect(LeafReaderContext context) {
        return load(context);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw sortException();
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        throw sortException();
    }

    protected abstract IllegalArgumentException sortException();

    /** helper: checks a fieldinfo and throws exception if it's definitely not a LatLonDocValuesField */
    protected static void checkCompatible(FieldInfo fieldInfo, String fieldTypeName) {
        // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
        if (fieldInfo.getDocValuesType() != DocValuesType.NONE && fieldInfo.getDocValuesType() != DocValuesType.BINARY) {
            throw new IllegalArgumentException(
                "field=\""
                    + fieldInfo.name
                    + "\" was indexed with docValuesType="
                    + fieldInfo.getDocValuesType()
                    + " but this type has docValuesType="
                    + DocValuesType.BINARY
                    + ", is the field really a "
                    + fieldTypeName
                    + " field?"
            );
        }
    }
}
