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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;

/**
 * A stateful lightweight per document geo values.
 */
public abstract class GeoShapeValues extends ShapeValues<GeoShapeValues.GeoShapeValue> {

    public static final GeoShapeValues EMPTY = new GeoShapeValues() {
        private final GeoShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = GeoShapeValuesSourceType.instance();

        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return DEFAULT_VALUES_SOURCE_TYPE;
        }

        @Override
        public GeoShapeValue value() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Creates a new {@link GeoShapeValues} instance
     */
    protected GeoShapeValues() {
        super(CoordinateEncoder.GEO, GeoShapeValues.GeoShapeValue::new, new GeoShapeIndexer(Orientation.CCW, "missing"));
    }

    /**
     * Geo data is limited to geographic lat/lon degrees, so we use the GeographyValidator
     */
    public GeometryValidator geometryValidator() {
        return GeographyValidator.instance(true);
    }

    /**
     * thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using the Geo decoder
     */
    public static class GeoShapeValue extends ShapeValues.ShapeValue {
        private final Tile2DVisitor tile2DVisitor;  // This does not work for cartesian, so we currently only support this in geo

        public GeoShapeValue() {
            super(CoordinateEncoder.GEO, (x, y) -> new GeoPoint(y, x));
            this.tile2DVisitor = new Tile2DVisitor();
        }

        @SuppressWarnings("this-escape")
        public GeoShapeValue(StreamInput in) throws IOException {
            this();
            reset(in);
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
            visit(tile2DVisitor);
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

        @Override
        public String getWriteableName() {
            return "GeoShapeValue";
        }
    }
}
