/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPoint;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.CartesianShapeIndexer;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;

import java.io.IOException;

/**
 * A stateful lightweight per document cartesian values.
 */
public abstract class CartesianShapeValues extends ShapeValues<CartesianShapeValues.CartesianShapeValue> {

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
        super(CoordinateEncoder.CARTESIAN, CartesianShapeValues.CartesianShapeValue::new, new CartesianShapeIndexer("missing"));
    }

    /**
     * Cartesian data is not limited to geographic lat/lon degrees, so we use the standard validator
     */
    public GeometryValidator geometryValidator() {
        return StandardValidator.instance(true);
    }

    /**
     * thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using the cartesian decoder
     */
    public static class CartesianShapeValue extends ShapeValues.ShapeValue {

        public CartesianShapeValue() {
            super(CoordinateEncoder.CARTESIAN, CartesianPoint::new);
        }

        @Override
        protected Component2D centroidAsComponent2D() throws IOException {
            return XYGeometry.create(new XYPoint((float) getX(), (float) getY()));
        }

        /**
         * Determine the {@link GeoRelation} between the current shape and a {@link XYGeometry}. It only supports
         * simple geometries, therefore it will fail if the LatLonGeometry is a {@link org.apache.lucene.geo.Rectangle}
         * that crosses the dateline.
         * TODO: this is test only method, perhaps should be moved to test code
         */
        public GeoRelation relate(XYGeometry geometry) throws IOException {
            return relate(XYGeometry.create(geometry));
        }
    }
}
