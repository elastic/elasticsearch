/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

/**
 * A stateful lightweight per document set of geo values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiGeoValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoValue value = values.valueAt(i);
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public abstract class MultiGeoShapeValues {

    public static MultiGeoShapeValues EMPTY = new MultiGeoShapeValues() {
        private GeoShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = GeoShapeValuesSourceType.instance();
        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public int docValueCount() {
            return 0;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return DEFAULT_VALUES_SOURCE_TYPE;
        }

        @Override
        public GeoShapeValue nextValue() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Creates a new {@link MultiGeoShapeValues} instance
     */
    protected MultiGeoShapeValues() {
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Return the number of geo points the current document has.
     */
    public abstract int docValueCount();

    public abstract ValuesSourceType valuesSourceType();

    /**
     * Return the next value associated with the current document. This must not be
     * called more than {@link #docValueCount()} times.
     *
     * Note: the returned {@link GeoShapeValue} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeoShapeValue nextValue() throws IOException;

    public static class GeoShapeValue {
        private static final WellKnownText MISSING_GEOMETRY_PARSER = new WellKnownText(true, new GeographyValidator(true));

        private final TriangleTreeReader reader;
        private final BoundingBox boundingBox;

        public GeoShapeValue(TriangleTreeReader reader)  {
            this.reader = reader;
            this.boundingBox = new BoundingBox();
        }

        public BoundingBox boundingBox() {
            boundingBox.reset(reader.getExtent(), GeoShapeCoordinateEncoder.INSTANCE);
            return boundingBox;
        }

        /**
         * @return the latitude of the centroid of the shape
         */
        public GeoRelation relate(Rectangle rectangle) {
            int minX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMinX());
            int maxX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMaxX());
            int minY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMinY());
            int maxY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMaxY());
            return reader.relateTile(minX, minY, maxX, maxY);
        }

        public DimensionalShapeType dimensionalShapeType() {
            return reader.getDimensionalShapeType();
        }

        public double weight() {
            return reader.getSumCentroidWeight();
        }

        public double lat() {
            return reader.getCentroidY();
        }

        /**
         * @return the longitude of the centroid of the shape
         */
        public double lon() {
            return reader.getCentroidX();
        }

        public static GeoShapeValue missing(String missing) {
            try {
                Geometry geometry = MISSING_GEOMETRY_PARSER.fromWKT(missing);
                ShapeField.DecodedTriangle[] triangles = toDecodedTriangles(geometry);
                TriangleTreeWriter writer =
                    new TriangleTreeWriter(Arrays.asList(triangles), GeoShapeCoordinateEncoder.INSTANCE,
                        new CentroidCalculator(geometry));
                ByteBuffersDataOutput output = new ByteBuffersDataOutput();
                writer.writeTo(output);
                TriangleTreeReader reader = new TriangleTreeReader(GeoShapeCoordinateEncoder.INSTANCE);
                reader.reset(new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size())));
                return new GeoShapeValue(reader);
            } catch (IOException | ParseException e) {
                throw new IllegalArgumentException("Can't apply missing value [" + missing + "]", e);
            }
        }

        private static ShapeField.DecodedTriangle[] toDecodedTriangles(Geometry geometry)  {
            GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
            geometry = indexer.prepareForIndexing(geometry);
            List<IndexableField> fields = indexer.indexShape(null, geometry);
            ShapeField.DecodedTriangle[] triangles = new ShapeField.DecodedTriangle[fields.size()];
            final byte[] scratch = new byte[7 * Integer.BYTES];
            for (int i = 0; i < fields.size(); i++) {
                BytesRef bytesRef = fields.get(i).binaryValue();
                assert bytesRef.length == 7 * Integer.BYTES;
                System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
                ShapeField.decodeTriangle(scratch, triangles[i] = new ShapeField.DecodedTriangle());
            }
            return triangles;
        }
    }

    public static class BoundingBox {
        public double top;
        public double bottom;
        public double negLeft;
        public double negRight;
        public double posLeft;
        public double posRight;

        private BoundingBox() {
        }

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

        private void reset(GeoPoint point) {
            this.top = point.lat();
            this.bottom = point.lat();
            if (point.lon() < 0) {
                this.negLeft = point.lon();
                this.negRight = point.lon();
                this.posLeft = Double.POSITIVE_INFINITY;
                this.posRight = Double.NEGATIVE_INFINITY;
            } else {
                this.negLeft = Double.POSITIVE_INFINITY;
                this.negRight = Double.NEGATIVE_INFINITY;
                this.posLeft = point.lon();
                this.posRight = point.lon();
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
