/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

public class GeoShapeValuesGenericWriteableTests extends ShapeValuesGenericWriteableTests<GeoShapeValues.GeoShapeValue> {

    @Override
    protected String shapeValueName() {
        return "GeoShapeValue";
    }

    @Override
    protected GenericWriteableWrapper createTestInstance() {
        try {
            GeoBoundingBox bbox = GeoTestUtils.randomBBox();
            Rectangle rectangle = new Rectangle(bbox.left(), bbox.right(), bbox.top(), bbox.bottom());
            GeoShapeValues.GeoShapeValue shapeValue = GeoTestUtils.geoShapeValue(rectangle);
            return new GenericWriteableWrapper(shapeValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected GenericWriteableWrapper mutateInstance(GenericWriteableWrapper instance) throws IOException {
        ShapeValues.ShapeValue shapeValue = instance.shapeValue();
        ShapeValues.BoundingBox bbox = shapeValue.boundingBox();
        double height = bbox.maxY() - bbox.minY();
        double width = bbox.maxX() - bbox.minX();
        double xs = width * 0.001;
        double ys = height * 0.001;
        Rectangle rectangle = new Rectangle(bbox.minX() + xs, bbox.maxX() - xs, bbox.maxY() - ys, bbox.minY() + ys);
        return new GenericWriteableWrapper(GeoTestUtils.geoShapeValue(rectangle));
    }
}
