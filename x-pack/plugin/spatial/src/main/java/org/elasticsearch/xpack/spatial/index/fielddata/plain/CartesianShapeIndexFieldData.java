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
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;

public class CartesianShapeIndexFieldData extends AbstractShapeIndexFieldData<CartesianShapeValues> {
    public CartesianShapeIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<CartesianShapeValues> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory);
    }

    @Override
    public LeafShapeFieldData<CartesianShapeValues> load(LeafReaderContext context) {
        LeafReader reader = context.reader();
        FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
        if (info != null) {
            checkCompatible(info, "shape");
        }
        return new CartesianShapeDVAtomicShapeFieldData(reader, fieldName, toScriptFieldFactory);
    }

    @Override
    protected IllegalArgumentException sortException() {
        return new IllegalArgumentException("can't sort on shape field without using specific sorting feature, like cartesian_distance");
    }
}
