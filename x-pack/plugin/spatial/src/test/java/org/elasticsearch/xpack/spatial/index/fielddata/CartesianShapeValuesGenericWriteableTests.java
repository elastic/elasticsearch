/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

public class CartesianShapeValuesGenericWriteableTests extends ShapeValuesGenericWriteableTests<CartesianShapeValues.CartesianShapeValue> {

    @Override
    protected String shapeValueName() {
        return "CartesianShapeValue";
    }

    @Override
    protected GenericWriteableWrapper createTestInstance() {
        try {
            double minX = randomDoubleBetween(-Float.MAX_VALUE, 0, false);
            double minY = randomDoubleBetween(-Float.MAX_VALUE, 0, false);
            double maxX = randomDoubleBetween(minX + 10, Float.MAX_VALUE, false);
            double maxY = randomDoubleBetween(minY + 10, Float.MAX_VALUE, false);
            Rectangle rectangle = new Rectangle(minX, maxX, maxY, minY);
            CartesianShapeValues.CartesianShapeValue shapeValue = GeoTestUtils.cartesianShapeValue(rectangle);
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
        return new GenericWriteableWrapper(GeoTestUtils.cartesianShapeValue(rectangle));
    }
}
