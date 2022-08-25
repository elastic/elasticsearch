/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryShapeDocValuesField;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.text.ParseException;

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
public abstract class GeoShapeValues {

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
    protected GeoShapeValues() {}

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    public abstract ValuesSourceType valuesSourceType();

    /**
     * Return the value associated with the current document.
     *
     * Note: the returned {@link GeoShapeValue} might be shared across invocations.
     *
     * @return the value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeoShapeValue value() throws IOException;

    /** thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the Geo decoder */
    public static class GeoShapeValue implements ToXContentFragment {
        private static final GeoShapeIndexer MISSING_GEOSHAPE_INDEXER = new GeoShapeIndexer(Orientation.CCW, "missing");
        private final GeometryDocValueReader reader;
        private final BoundingBox boundingBox;
        private final Tile2DVisitor tile2DVisitor;
        private final LatLonGeometryRelationVisitor component2DRelationVisitor;

        public GeoShapeValue() {
            this.reader = new GeometryDocValueReader();
            this.boundingBox = new BoundingBox();
            this.tile2DVisitor = new Tile2DVisitor();
            this.component2DRelationVisitor = new LatLonGeometryRelationVisitor(CoordinateEncoder.GEO);
        }

        /**
         * reset the geometry.
         */
        public void reset(BytesRef bytesRef) throws IOException {
            this.reader.reset(bytesRef);
            this.boundingBox.reset(reader.getExtent(), CoordinateEncoder.GEO);
        }

        public BoundingBox boundingBox() {
            return boundingBox;
        }

        /**
         * Select a label position that is within the shape.
         */
        public GeoPoint labelPosition() throws IOException {
            // For polygons we prefer to use the centroid, as long as it is within the polygon
            if (reader.getDimensionalShapeType() == DimensionalShapeType.POLYGON) {
                Component2DVisitor visitor = Component2DVisitor.getVisitor(
                    LatLonGeometry.create(new Point(lat(), lon())),
                    ShapeField.QueryRelation.INTERSECTS,
                    CoordinateEncoder.GEO
                );
                reader.visit(visitor);
                if (visitor.matches()) {
                    return new GeoPoint(lat(), lon());
                }
            }
            // For all other cases, use the first triangle (or line or point) in the tree which will always intersect the shape
            LabelPositionVisitor<GeoPoint> visitor = new LabelPositionVisitor<>(CoordinateEncoder.GEO, (x, y) -> new GeoPoint(y, x));
            reader.visit(visitor);
            return visitor.labelPosition();
        }

        /**
         * Determine the {@link GeoRelation} between the current shape and a bounding box provided in
         * the encoded space.
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
         */
        public GeoRelation relate(LatLonGeometry latLonGeometry) throws IOException {
            component2DRelationVisitor.reset(latLonGeometry);
            reader.visit(component2DRelationVisitor);
            return component2DRelationVisitor.relation();
        }

        public DimensionalShapeType dimensionalShapeType() {
            return reader.getDimensionalShapeType();
        }

        public double weight() throws IOException {
            return reader.getSumCentroidWeight();
        }

        /**
         * @return the latitude of the centroid of the shape
         */
        public double lat() throws IOException {
            return CoordinateEncoder.GEO.decodeY(reader.getCentroidY());
        }

        /**
         * @return the longitude of the centroid of the shape
         */
        public double lon() throws IOException {
            return CoordinateEncoder.GEO.decodeX(reader.getCentroidX());
        }

        public static GeoShapeValue missing(String missing) {
            try {
                final Geometry geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), true, missing);
                final BinaryShapeDocValuesField field = new BinaryShapeDocValuesField("missing", CoordinateEncoder.GEO);
                field.add(MISSING_GEOSHAPE_INDEXER.indexShape(geometry), geometry);
                final GeoShapeValue value = new GeoShapeValue();
                value.reset(field.binaryValue());
                return value;
            } catch (IOException | ParseException e) {
                throw new IllegalArgumentException("Can't apply missing value [" + missing + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalArgumentException("cannot write xcontent for geo_shape doc value");
        }
    }

    public static class BoundingBox {
        public double top;
        public double bottom;
        public double negLeft;
        public double negRight;
        public double posLeft;
        public double posRight;

        private BoundingBox() {}

        private void reset(Extent extent, CoordinateEncoder coordinateEncoder) {
            this.top = coordinateEncoder.decodeY(extent.top);
            this.bottom = coordinateEncoder.decodeY(extent.bottom);

            if (extent.negLeft == Integer.MAX_VALUE && extent.negRight == Integer.MIN_VALUE) {
                this.negLeft = Double.POSITIVE_INFINITY;
                this.negRight = Double.NEGATIVE_INFINITY;
            } else {
                this.negLeft = coordinateEncoder.decodeX(extent.negLeft);
                this.negRight = coordinateEncoder.decodeX(extent.negRight);
            }

            if (extent.posLeft == Integer.MAX_VALUE && extent.posRight == Integer.MIN_VALUE) {
                this.posLeft = Double.POSITIVE_INFINITY;
                this.posRight = Double.NEGATIVE_INFINITY;
            } else {
                this.posLeft = coordinateEncoder.decodeX(extent.posLeft);
                this.posRight = coordinateEncoder.decodeX(extent.posRight);
            }
        }

        /**
         * @return the minimum y-coordinate of the extent
         */
        public double minY() {
            return bottom;
        }

        /**
         * @return the maximum y-coordinate of the extent
         */
        public double maxY() {
            return top;
        }

        /**
         * @return the absolute minimum x-coordinate of the extent, whether it is positive or negative.
         */
        public double minX() {
            return Math.min(negLeft, posLeft);
        }

        /**
         * @return the absolute maximum x-coordinate of the extent, whether it is positive or negative.
         */
        public double maxX() {
            return Math.max(negRight, posRight);
        }
    }
}
