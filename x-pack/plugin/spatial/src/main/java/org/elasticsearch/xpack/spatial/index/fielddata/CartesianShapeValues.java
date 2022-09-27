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
import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.CartesianShapeIndexer;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;

import java.io.IOException;

/**
 * A set of factory methods for generating versions of ShapeValues that are specific to cartesian point and shape data.
 */
public interface CartesianShapeValues extends ShapeValues<CartesianShapeValues.CartesianShapeValue> {

    CartesianShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = CartesianShapeValuesSourceType.instance();
    CartesianShapeValues EMPTY = new CartesianShapeValues.Empty();

    /**
     * Produce empty ShapeValues for cartesian data
     */
    class Empty extends ShapeValues.Empty<CartesianShapeValues.CartesianShapeValue> implements CartesianShapeValues {
        Empty() {
            super(CoordinateEncoder.CARTESIAN, CartesianShapeValue::new, new CartesianShapeIndexer("missing"), DEFAULT_VALUES_SOURCE_TYPE);
        }
    }

    /**
     * Produce ShapeValues for geo data that wrap existing ShapeValues but replace missing fields with the specified value
     */
    class Wrapped extends ShapeValues.Wrapped<CartesianShapeValue> implements CartesianShapeValues {
        public Wrapped(ShapeValues<CartesianShapeValue> values, CartesianShapeValue missing) {
            super(
                CoordinateEncoder.CARTESIAN,
                CartesianShapeValue::new,
                new CartesianShapeIndexer("missing"),
                DEFAULT_VALUES_SOURCE_TYPE,
                values,
                missing
            );
        }
    }

    /**
     * Produce ShapeValues for geo data that generate each ShapeValue from the specified BinaryDocValues
     */
    class BinaryDocData extends ShapeValues.BinaryDocData<CartesianShapeValue> implements CartesianShapeValues {
        public BinaryDocData(BinaryDocValues binaryValues) {
            super(
                CoordinateEncoder.CARTESIAN,
                CartesianShapeValue::new,
                new CartesianShapeIndexer("missing"),
                DEFAULT_VALUES_SOURCE_TYPE,
                binaryValues,
                new CartesianShapeValue()
            );
        }
    }

    /**
     * thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using the cartesian decoder
     */
    class CartesianShapeValue extends ShapeValues.ShapeValue {
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
