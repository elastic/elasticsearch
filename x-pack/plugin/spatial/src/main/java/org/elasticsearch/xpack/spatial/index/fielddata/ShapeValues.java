/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryShapeDocValuesField;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.function.Supplier;

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
public abstract class ShapeValues<T extends ToXContentFragment> {
    protected final CoordinateEncoder encoder;
    protected final Supplier<ShapeValue<T>> supplier;
    protected final ShapeIndexer missingShapeIndexer;

    /**
     * Creates a new {@link ShapeValues} instance
     */
    protected ShapeValues(CoordinateEncoder encoder, Supplier<ShapeValue<T>> supplier, ShapeIndexer missingShapeIndexer) {
        this.encoder = encoder;
        this.supplier = supplier;
        this.missingShapeIndexer = missingShapeIndexer;
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    public abstract ValuesSourceType valuesSourceType();

    /**
     * Return the value associated with the current document.
     *
     * Note: the returned {@link ShapeValue<T>} might be shared across invocations.
     *
     * @return the value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract ShapeValue<T> value() throws IOException;

    public ShapeValue<T> missing(String missing) {
        try {
            final Geometry geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), true, missing);
            final BinaryShapeDocValuesField field = new BinaryShapeDocValuesField("missing", encoder);
            field.add(missingShapeIndexer.indexShape(geometry), geometry);
            final ShapeValue<T> value = supplier.get();
            value.reset(field.binaryValue());
            return value;
        } catch (IOException | ParseException e) {
            throw new IllegalArgumentException("Can't apply missing value [" + missing + "]", e);
        }
    }

    /** thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the Geo decoder */
    public abstract static class ShapeValue<T extends ToXContentFragment> implements ToXContentFragment {
        protected final GeometryDocValueReader reader;
        private final BoundingBox boundingBox;
        private final Tile2DVisitor tile2DVisitor;
        protected final CoordinateEncoder encoder;

        public ShapeValue(CoordinateEncoder encoder) {
            this.reader = new GeometryDocValueReader();
            this.boundingBox = new BoundingBox();
            this.tile2DVisitor = new Tile2DVisitor();
            this.encoder = encoder;
        }

        /**
         * reset the geometry.
         */
        public void reset(BytesRef bytesRef) throws IOException {
            this.reader.reset(bytesRef);
            this.boundingBox.reset(reader.getExtent(), encoder);
        }

        public BoundingBox boundingBox() {
            return boundingBox;
        }

        /**
         * Select a label position that is within the shape.
         */
        public abstract Location labelPosition() throws IOException;

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
         * Determine if the current shape value intersects the specified geometry.
         * Note that the intersection must be true in quantized space, so it is possible that
         * points on the edges of geometries will return false due to quantization shifting them off the geometry.
         * To deal with this, one option is to pass in a circle around the point with a 1m radius
         * which is enough to cover the resolution of the quantization.
         */
        public boolean intersects(Geometry geometry) throws IOException {
            Component2D component = null;
            // TODO: create Component2D for incoming geometry
            Component2DVisitor visitor = Component2DVisitor.getVisitor(
                component,
                ShapeField.QueryRelation.INTERSECTS,
                CoordinateEncoder.Cartesian
            );
            reader.visit(visitor);
            return visitor.matches();
        }

        public DimensionalShapeType dimensionalShapeType() {
            return reader.getDimensionalShapeType();
        }

        public double weight() throws IOException {
            return reader.getSumCentroidWeight();
        }

        /**
         * @return the Y-value (or latitude) of the centroid of the shape
         */
        public double getY() throws IOException {
            return encoder.decodeY(reader.getCentroidY());
        }

        /**
         * @return the X-value (or longitude) of the centroid of the shape
         */
        public double getX() throws IOException {
            return encoder.decodeX(reader.getCentroidX());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalArgumentException("cannot write xcontent for geo_shape doc value");
        }
    }

    public static class Location {
        private double x;
        private double y;

        Location(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return x;
        }

        public double getY() {
            return y;
        }

        public double getLat() {
            return y;
        }

        public double getLon() {
            return x;
        }
    }

    public static class BoundingBox {
        public double top;
        public double bottom;
        public double negLeft;
        public double negRight;
        public double posLeft;
        public double posRight;

        public BoundingBox() {}

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
