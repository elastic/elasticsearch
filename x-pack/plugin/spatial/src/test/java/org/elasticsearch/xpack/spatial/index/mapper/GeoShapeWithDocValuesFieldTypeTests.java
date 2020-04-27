/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Before;

public class GeoShapeWithDocValuesFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new FieldTypeTestCase.Modifier("orientation", true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType)ft).setOrientation(ShapeBuilder.Orientation.LEFT);
            }
        });
    }
}
