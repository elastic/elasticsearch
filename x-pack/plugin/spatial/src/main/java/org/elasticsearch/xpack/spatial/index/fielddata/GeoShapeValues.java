/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;

/**
 * A stateful lightweight per document geo values.
 */
public interface GeoShapeValues extends ShapeValues<GeoShapeValues.GeoShapeValue> {

    GeoShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = GeoShapeValuesSourceType.instance();
    GeoShapeValues EMPTY = new Empty();

    /**
     * Produce empty ShapeValues for geo data
     */
    class Empty extends ShapeValues.Empty<GeoShapeValue> implements GeoShapeValues {
        Empty() {
            super(CoordinateEncoder.GEO, GeoShapeValue::new, new GeoShapeIndexer(Orientation.CCW, "missing"), DEFAULT_VALUES_SOURCE_TYPE);
        }
    }

    /**
     * Produce ShapeValues for geo data that wrap existing ShapeValues but replace missing fields with the specified value
     */
    class Wrapped extends ShapeValues.Wrapped<GeoShapeValue> implements GeoShapeValues {
        public Wrapped(ShapeValues<GeoShapeValue> values, GeoShapeValue missing) {
            super(
                CoordinateEncoder.GEO,
                GeoShapeValue::new,
                new GeoShapeIndexer(Orientation.CCW, "missing"),
                DEFAULT_VALUES_SOURCE_TYPE,
                values,
                missing
            );
        }
    }

    /**
    * Produce ShapeValues for geo data that generate each ShapeValue from the specified BinaryDocValues
    */
    class BinaryDocData extends ShapeValues.BinaryDocData<GeoShapeValue> implements GeoShapeValues {
        public BinaryDocData(BinaryDocValues binaryValues) {
            super(
                CoordinateEncoder.GEO,
                GeoShapeValue::new,
                new GeoShapeIndexer(Orientation.CCW, "missing"),
                DEFAULT_VALUES_SOURCE_TYPE,
                binaryValues,
                new GeoShapeValue()
            );
        }
    }

    /**
     * thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using the Geo decoder
     */
    class GeoShapeValue extends ShapeValues.ShapeValue {
        private final Tile2DVisitor tile2DVisitor;  // This does not work for cartesian, so we currently only support this in geo

        public GeoShapeValue() {
            super(CoordinateEncoder.GEO, (x, y) -> new GeoPoint(y, x));
            this.tile2DVisitor = new Tile2DVisitor();
        }

        @Override
        protected Component2D centroidAsComponent2D() throws IOException {
            return LatLonGeometry.create(new Point(getY(), getX()));
        }

        /**
         * Determine the {@link GeoRelation} between the current shape and a bounding box provided in
         * the encoded space. This does not work for cartesian, so we currently only support this in geo.
         */
        public GeoRelation relate(int minX, int maxX, int minY, int maxY) throws IOException {
            tile2DVisitor.reset(minX, minY, maxX, maxY);
            reader.visit(tile2DVisitor);
            return tile2DVisitor.relation();
        }

        /**
         * Determine the {@link GeoRelation} between the current shape and a {@link LatLonGeometry}. It only supports
         * simple geometries, therefore it will fail if the LatLonGeometry is a {@link org.apache.lucene.geo.Rectangle}
         * that crosses the dateline.
         * TODO: this is a test only method, perhaps should be moved to test code
         */
        public GeoRelation relate(LatLonGeometry geometry) throws IOException {
            return relate(LatLonGeometry.create(geometry));
        }
    }
}
