/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.CentroidCalculator;
import org.elasticsearch.common.geo.CoordinateEncoder;
import org.elasticsearch.common.geo.DimensionalShapeType;
import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.TriangleTreeReader;
import org.elasticsearch.common.geo.TriangleTreeWriter;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

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
public abstract class MultiGeoValues {

    /**
     * Creates a new {@link MultiGeoValues} instance
     */
    protected MultiGeoValues() {
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
     * Note: the returned {@link GeoValue} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeoValue nextValue() throws IOException;

    public static class GeoPointValue implements GeoValue {
        private final GeoPoint geoPoint;
        private final BoundingBox boundingBox;

        public GeoPointValue(GeoPoint geoPoint) {
            this.boundingBox = new BoundingBox();
            this.geoPoint = geoPoint;
        }

        public GeoPoint geoPoint() {
            return geoPoint;
        }

        @Override
        public BoundingBox boundingBox() {
            boundingBox.reset(geoPoint);
            return boundingBox;
        }

        @Override
        public GeoRelation relate(Rectangle rectangle) {
            if (geoPoint.lat() >= rectangle.getMinLat() && geoPoint.lat() <= rectangle.getMaxLat()
                    && geoPoint.lon() >= rectangle.getMinLon() && geoPoint.lon() <= rectangle.getMaxLon()) {
                return GeoRelation.QUERY_CROSSES;
            }
            return GeoRelation.QUERY_DISJOINT;
        }

        @Override
        public DimensionalShapeType dimensionalShapeType() {
            return DimensionalShapeType.POINT;
        }

        @Override
        public double weight() {
            return 1.0;
        }

        @Override
        public double lat() {
            return geoPoint.lat();
        }

        @Override
        public double lon() {
            return geoPoint.lon();
        }

        @Override
        public String toString() {
            return geoPoint.toString();
        }
    }

    public static class GeoShapeValue implements GeoValue {
        private static final WellKnownText MISSING_GEOMETRY_PARSER = new WellKnownText(true, new GeographyValidator(true));

        private final TriangleTreeReader reader;
        private final BoundingBox boundingBox;

        public GeoShapeValue(TriangleTreeReader reader)  {
            this.reader = reader;
            this.boundingBox = new BoundingBox();
        }

        @Override
        public BoundingBox boundingBox() {
            boundingBox.reset(reader.getExtent(), GeoShapeCoordinateEncoder.INSTANCE);
            return boundingBox;
        }

        /**
         * @return the latitude of the centroid of the shape
         */
        @Override
        public GeoRelation relate(Rectangle rectangle) {
            int minX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMinX());
            int maxX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMaxX());
            int minY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMinY());
            int maxY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMaxY());
            return reader.relateTile(minX, minY, maxX, maxY);
        }

        @Override
        public DimensionalShapeType dimensionalShapeType() {
            return reader.getDimensionalShapeType();
        }

        @Override
        public double weight() {
            return reader.getSumCentroidWeight();
        }

        @Override
        public double lat() {
            return reader.getCentroidY();
        }

        /**
         * @return the longitude of the centroid of the shape
         */
        @Override
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

    /**
     * interface for geo-shape and geo-point doc-values to
     * retrieve properties used in aggregations.
     */
    public interface GeoValue {
        double lat();
        double lon();
        BoundingBox boundingBox();
        GeoRelation relate(Rectangle rectangle);
        DimensionalShapeType dimensionalShapeType();
        double weight();
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
