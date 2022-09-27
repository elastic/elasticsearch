/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryShapeDocValuesField;
import org.elasticsearch.xpack.spatial.search.aggregations.support.ShapeValuesSourceType;

import java.io.IOException;
import java.text.ParseException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A stateful lightweight per document geo values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   ShapeValues values = ..;
 *   // for each docID
 *   if (values.advanceExact(docId)) {
 *     ShapeValue value = values.value()
 *     final int numValues = values.count();
 *     // process value
 *   }
 * </pre>
 *
 * There is just one value for one document.
 */
public interface ShapeValues<T extends ShapeValues.ShapeValue> {

    /**
     * Advance this instance to the given document id
     *
     * @return true if there is a value for this document
     */
    boolean advanceExact(int doc) throws IOException;

    /**
     * Return the value associated with the current document.
     * <p>
     * Note: the returned {@link ShapeValue} might be shared across invocations.
     *
     * @return the value for the current docID set to {@link #advanceExact(int)}.
     */
    T value() throws IOException;

    ValuesSourceType valuesSourceType();

    T missing(String missing);

    abstract class ShapeValuesImpl<T extends ShapeValues.ShapeValue> implements ShapeValues<T> {
        protected final CoordinateEncoder encoder;
        protected final Supplier<T> supplier;
        protected final ShapeIndexer missingShapeIndexer;
        protected final ShapeValuesSourceType valuesSourceType;

        protected ShapeValuesImpl(
            CoordinateEncoder encoder,
            Supplier<T> supplier,
            ShapeIndexer missingShapeIndexer,
            ShapeValuesSourceType valuesSourceType
        ) {
            this.encoder = encoder;
            this.supplier = supplier;
            this.missingShapeIndexer = missingShapeIndexer;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return valuesSourceType;
        }

        @Override
        public T missing(String missing) {
            try {
                final Geometry geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), true, missing);
                final BinaryShapeDocValuesField field = new BinaryShapeDocValuesField("missing", encoder);
                field.add(missingShapeIndexer.indexShape(geometry), geometry);
                final T value = supplier.get();
                value.reset(field.binaryValue());
                return value;
            } catch (IOException | ParseException e) {
                throw new IllegalArgumentException("Can't apply missing value [" + missing + "]", e);
            }
        }
    }

    /**
     * An empty collection of ShapeValue
     */
    class Empty<T extends ShapeValues.ShapeValue> extends ShapeValuesImpl<T> {
        protected Empty(CoordinateEncoder encoder, Supplier<T> supplier, ShapeIndexer indexer, ShapeValuesSourceType sourceType) {
            super(encoder, supplier, indexer, sourceType);
        }

        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public T value() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * ShapeValues that produces a stream of ShapeValue from a BinaryDocValues instance
     */
    class BinaryDocData<T extends ShapeValues.ShapeValue> extends ShapeValuesImpl<T> {
        private final BinaryDocValues binaryValues;
        private final T shapeValue;

        protected BinaryDocData(
            CoordinateEncoder encoder,
            Supplier<T> supplier,
            ShapeIndexer indexer,
            ShapeValuesSourceType sourceType,
            BinaryDocValues binaryValues,
            T shapeValue
        ) {
            super(encoder, supplier, indexer, sourceType);
            this.binaryValues = binaryValues;
            this.shapeValue = shapeValue;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return binaryValues.advanceExact(doc);
        }

        @Override
        public T value() throws IOException {
            shapeValue.reset(binaryValues.binaryValue());
            return shapeValue;
        }
    }

    /**
     * ShapeValues that wraps another ShapeValues, but replaces missing values with the specified missing value
     */
    class Wrapped<T extends ShapeValues.ShapeValue> extends ShapeValuesImpl<T> {
        private final ShapeValues<T> values;
        private final T missing;
        private boolean exists;

        protected Wrapped(
            CoordinateEncoder encoder,
            Supplier<T> supplier,
            ShapeIndexer indexer,
            ShapeValuesSourceType sourceType,
            ShapeValues<T> values,
            T missing
        ) {
            super(encoder, supplier, indexer, sourceType);
            this.values = values;
            this.missing = missing;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            exists = values.advanceExact(doc);
            // always return true because we want to return a value even if the document does not have a value
            return true;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return values.valuesSourceType();
        }

        @Override
        public T value() throws IOException {
            return exists ? values.value() : missing;
        }

        @Override
        public String toString() {
            return "anon ShapeValues of [" + super.toString() + "]";
        }
    }

    /**
     * thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the provided decoder (could be geo or cartesian)
     */
    abstract class ShapeValue implements ToXContentFragment {
        protected final GeometryDocValueReader reader;
        private final BoundingBox boundingBox;
        protected final CoordinateEncoder encoder;
        private final BiFunction<Double, Double, SpatialPoint> pointMaker;
        private final Component2DRelationVisitor component2DRelationVisitor;

        public ShapeValue(CoordinateEncoder encoder, BiFunction<Double, Double, SpatialPoint> pointMaker) {
            this.reader = new GeometryDocValueReader();
            this.boundingBox = new BoundingBox();
            this.encoder = encoder;
            this.pointMaker = pointMaker;
            this.component2DRelationVisitor = new Component2DRelationVisitor(encoder);
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

        protected abstract Component2D centroidAsComponent2D() throws IOException;

        private boolean centroidWithinShape() throws IOException {
            Component2DVisitor visitor = Component2DVisitor.getVisitor(
                centroidAsComponent2D(),
                ShapeField.QueryRelation.INTERSECTS,
                this.encoder
            );
            reader.visit(visitor);
            return visitor.matches();
        }

        /**
         * Select a label position that is within the shape.
         */
        public SpatialPoint labelPosition() throws IOException {
            // For polygons we prefer to use the centroid, as long as it is within the polygon
            if (reader.getDimensionalShapeType() == DimensionalShapeType.POLYGON && centroidWithinShape()) {
                return pointMaker.apply(getX(), getY());
            }
            // For all other cases, use the first triangle (or line or point) in the tree which will always intersect the shape
            LabelPositionVisitor visitor = new LabelPositionVisitor(this.encoder, pointMaker);
            reader.visit(visitor);
            return visitor.labelPosition();
        }

        /**
         * Determine the {@link GeoRelation} between the current shape and a {@link Component2D}. It only supports
         * simple geometries, therefore it will fail if the LatLonGeometry is a {@link org.apache.lucene.geo.Rectangle}
         * that crosses the dateline.
         */
        protected GeoRelation relate(Component2D component2D) throws IOException {
            component2DRelationVisitor.reset(component2D);
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

    class BoundingBox {
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
