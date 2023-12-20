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
    // maintain bwc by making centroid and bounding box available to ScriptDocValues.GeoPoints
    private CartesianPointScriptValues points = null;

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
        if (points == null) {
            points = new CartesianPointScriptValues(this);
        }

        return points;

    }

    public abstract static class CartesianGeometry<V> extends ScriptDocValues.BaseGeometry<CartesianPoint, V> {
        public CartesianGeometry(Supplier<V> supplier) {
            super(supplier);
        }

        /**
         * Returns the bounding box of this geometry
         */
        @Override
        public abstract CartesianBoundingBox getBoundingBox();

        /**
         * Returns the suggested label position
         */
        @Override
        public abstract CartesianPoint getLabelPosition();

        /**
         * Returns the centroid of this geometry
         */
        @Override
        public abstract CartesianPoint getCentroid();
    }

    // TODO: merge common code with ScriptValues.GeoPoint
    public class CartesianPointScriptValues extends CartesianGeometry<CartesianPoint> {

        private final GeometrySupplier<CartesianPoint, CartesianPoint> geometrySupplier;

        public CartesianPointScriptValues(GeometrySupplier<CartesianPoint, CartesianPoint> supplier) {
            super(supplier);
            geometrySupplier = supplier;
        }

        public CartesianPoint getValue() {
            return get(0);
        }

        public double getX() {
            return getValue().getX();
        }

        public double[] getXs() {
            double[] xs = new double[size()];
            for (int i = 0; i < size(); i++) {
                xs[i] = get(i).getX();
            }
            return xs;
        }

        public double[] getYs() {
            double[] ys = new double[size()];
            for (int i = 0; i < size(); i++) {
                ys[i] = get(i).getY();
            }
            return ys;
        }

        public double getY() {
            return getValue().getY();
        }

        @Override
        public CartesianPoint get(int index) {
            if (supplier.size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            final CartesianPoint point = supplier.getInternal(index);
            return new CartesianPoint(point.getX(), point.getY());
        }

        @Override
        public int size() {
            return supplier.size();
        }

        public double planeDistance(double x, double y) {
            CartesianPoint point = getValue();
            double dx = x - point.getX();
            double dy = y - point.getY();
            return Math.sqrt(dx * dx + dy * dy);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        @Override
        public int getDimensionalType() {
            return size() == 0 ? -1 : 0;
        }

        @Override
        public CartesianPoint getCentroid() {
            return size() == 0 ? null : geometrySupplier.getInternalCentroid();
        }

        @Override
        public CartesianBoundingBox getBoundingBox() {
            return size() == 0 ? null : (CartesianBoundingBox) geometrySupplier.getInternalBoundingBox();
        }

        @Override
        public CartesianPoint getLabelPosition() {
            return size() == 0 ? null : geometrySupplier.getInternalLabelPosition();
        }
    }
}
