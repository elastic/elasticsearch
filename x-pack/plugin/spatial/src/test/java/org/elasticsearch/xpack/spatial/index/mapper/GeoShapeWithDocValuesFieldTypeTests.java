/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType;
import org.junit.Before;

public class GeoShapeWithDocValuesFieldTypeTests extends FieldTypeTestCase<GeoShapeWithDocValuesFieldType> {

    @Before
    public void addModifiers() {
        addModifier(t -> {
            GeoShapeWithDocValuesFieldType copy = t.clone();
            if (copy.orientation() == ShapeBuilder.Orientation.RIGHT) {
                copy.setOrientation(ShapeBuilder.Orientation.LEFT);
            } else {
                copy.setOrientation(ShapeBuilder.Orientation.RIGHT);
            }
            return copy;
        });
    }

    @Override
    protected GeoShapeWithDocValuesFieldType createDefaultFieldType() {
        return new GeoShapeWithDocValuesFieldType();
    }
}
