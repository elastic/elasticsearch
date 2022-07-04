/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.script.field;

import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.PointDocValuesField;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

public class CartesianPointDocValuesField extends PointDocValuesField<CartesianPoint> {

    public CartesianPointDocValuesField(MultiPointValues<CartesianPoint> input, String name) {
        super(
            input,
            name,
            CartesianPoint::new,
            new CartesianBoundingBox(new CartesianPoint(), new CartesianPoint()),
            new CartesianPoint[0]
        );
    }

    @Override
    protected void resetPointAt(int i, CartesianPoint point) {
        values[i].reset(point.getX(), point.getY());
    }

    @Override
    protected void resetCentroidAndBounds(CartesianPoint point, CartesianPoint topLeft, CartesianPoint bottomRight) {
        centroid.reset(point.getX() / count, point.getY() / count);
        boundingBox.topLeft().reset(topLeft.getX(), topLeft.getY());
        boundingBox.bottomRight().reset(bottomRight.getX(), bottomRight.getY());
    }

    @Override
    protected double getXFrom(CartesianPoint point) {
        return point.getX();
    }

    @Override
    protected double getYFrom(CartesianPoint point) {
        return point.getY();
    }

    @Override
    protected CartesianPoint pointOf(double x, double y) {
        return new CartesianPoint(x, y);
    }

    @Override
    protected double planeDistance(double x1, double y1, CartesianPoint point) {
        double x = point.getX() - x1;
        double y = point.getY() - y1;
        return Math.sqrt(x * x + y * y);
    }

    @Override
    public ScriptDocValues<CartesianPoint> toScriptDocValues() {
        // TODO: Complete script support for Cartesian
        throw new IllegalStateException("unimplemented");
    }
}
