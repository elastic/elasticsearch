/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.LatLonGeometry;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

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
public abstract class GeoShapeValues extends ShapeValues<GeoPoint> {

    public static GeoShapeValues EMPTY = new GeoShapeValues() {
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
        super(CoordinateEncoder.GEO, GeoShapeValues.GeoShapeValue::new);
    }

    /** thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the Geo decoder */
    public static class GeoShapeValue extends ShapeValues.ShapeValue<GeoPoint> {
        public GeoShapeValue() {
            super(CoordinateEncoder.GEO);
        }

        /**
         * Select a label position that is within the shape.
         */
        public GeoPoint labelPosition() throws IOException {
            // For polygons we prefer to use the centroid, as long as it is within the polygon
            if (reader.getDimensionalShapeType() == DimensionalShapeType.POLYGON && intersects(new Point(getX(), getY()))) {
                // Note: GeoPoint has coordinates swapped when compared to Point
                return new GeoPoint(getY(), getX());
            }
            // For all other cases, use the first triangle (or line or point) in the tree which will always intersect the shape
            LabelPositionVisitor<GeoPoint> visitor = new LabelPositionVisitor<>(CoordinateEncoder.GEO, (x, y) -> new GeoPoint(y, x));
            reader.visit(visitor);
            return visitor.labelPosition();
        }

        /**
         * Determine if the current shape value intersects the specified geometry.
         * Note that the intersection must be true in quantized space, so it is possible that
         * points on the edges of geometries will return false due to quantization shifting them off the geometry.
         * To deal with this, one option is to pass in a circle around the point with a 1m radius
         * which is enough to cover the resolution of the quantization.
         */
        public boolean intersects(Geometry geometry) throws IOException {
            LatLonGeometry[] latLonGeometries = GeoShapeQueryable.toQuantizeLuceneGeometry(geometry, ShapeRelation.INTERSECTS);
            Component2DVisitor visitor = Component2DVisitor.getVisitor(
                LatLonGeometry.create(latLonGeometries),
                ShapeField.QueryRelation.INTERSECTS,
                CoordinateEncoder.GEO
            );
            reader.visit(visitor);
            return visitor.matches();
        }
    }
}
