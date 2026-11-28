/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.lucene.spatial.GeometryDocValueFormat;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;

public class LatLonShapeIndexFieldData extends AbstractShapeIndexFieldData<GeoShapeValues> {
    private final GeometryDocValueFormat docValueFormat;

    /**
     * @param docValueFormat the format of the field's doc values
     */
    public LatLonShapeIndexFieldData(
        String fieldName,
        GeometryDocValueFormat docValueFormat,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory);
        this.docValueFormat = docValueFormat;
    }

    @Override
    public LeafShapeFieldData<GeoShapeValues> load(LeafReaderContext context) {
        LeafReader reader = context.reader();
        FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
        if (info != null) {
            checkCompatible(info, "geo_shape");
        }
        return new LatLonShapeDVAtomicShapeFieldData(reader, fieldName, docValueFormat, toScriptFieldFactory);
    }

    @Override
    protected IllegalArgumentException sortException() {
        return new IllegalArgumentException("can't sort on geo_shape field without using specific sorting feature, like geo_distance");
    }
}
