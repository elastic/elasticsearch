/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

public class LatLonShapeIndexFieldData extends AbstractShapeIndexFieldData implements IndexShapeFieldData {
    public LatLonShapeIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<ShapeValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory);
    }

    @Override
    public LeafShapeFieldData load(LeafReaderContext context) {
        LeafReader reader = context.reader();
        FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
        if (info != null) {
            checkCompatible(info);
        }
        return new LatLonShapeDVAtomicShapeFieldData(reader, fieldName, toScriptFieldFactory);
    }

    @Override
    public LeafShapeFieldData loadDirect(LeafReaderContext context) {
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
        throw new IllegalArgumentException("can't sort on geo_shape field without using specific sorting feature, like geo_distance");
    }

    /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonDocValuesField */
    static void checkCompatible(FieldInfo fieldInfo) {
        // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
        if (fieldInfo.getDocValuesType() != DocValuesType.NONE && fieldInfo.getDocValuesType() != DocValuesType.BINARY) {
            throw new IllegalArgumentException(
                "field=\""
                    + fieldInfo.name
                    + "\" was indexed with docValuesType="
                    + fieldInfo.getDocValuesType()
                    + " but this type has docValuesType="
                    + DocValuesType.BINARY
                    + ", is the field really a geo-shape field?"
            );
        }
    }
}
