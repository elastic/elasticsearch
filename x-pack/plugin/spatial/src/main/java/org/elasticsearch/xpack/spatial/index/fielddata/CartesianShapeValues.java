/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.geometry.Point;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.CartesianShapeIndexer;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;

import java.io.IOException;

/**
 * A stateful lightweight per document geo values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiGeoValues values = ..;
 *   // for each docID
 *   if (values.advanceExact(docId)) {
 *     GeoValue value = values.value()
 *     final int numValues = values.count();
 *     // process value
 *   }
 * </pre>
 *
 * There is just one value for one document.
 */
public abstract class CartesianShapeValues extends ShapeValues<CartesianPoint> {

    public static CartesianShapeValues EMPTY = new CartesianShapeValues() {
        private final CartesianShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = CartesianShapeValuesSourceType.instance();

        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return DEFAULT_VALUES_SOURCE_TYPE;
        }

        @Override
        public CartesianShapeValue value() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Creates a new {@link CartesianShapeValues} instance
     */
    protected CartesianShapeValues() {
        super(CoordinateEncoder.Cartesian, CartesianShapeValues.CartesianShapeValue::new, new CartesianShapeIndexer("missing"));
    }

    /** thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the Geo decoder */
    public static class CartesianShapeValue extends ShapeValues.ShapeValue<CartesianPoint> {

        public CartesianShapeValue() {
            super(CoordinateEncoder.Cartesian);
        }

        /**
         * Select a label position that is within the shape.
         */
        public Location labelPosition() throws IOException {
            // For polygons we prefer to use the centroid, as long as it is within the polygon
            if (reader.getDimensionalShapeType() == DimensionalShapeType.POLYGON && intersects(new Point(getX(), getY()))) {
                return new Location(getX(), getY());
            }
            // For all other cases, use the first triangle (or line or point) in the tree which will always intersect the shape
            LabelPositionVisitor<CartesianPoint> visitor = new LabelPositionVisitor<>(encoder, CartesianPoint::new);
            reader.visit(visitor);
            CartesianPoint point = visitor.labelPosition();
            return new Location(point.getX(), point.getY());
        }
    }
}
