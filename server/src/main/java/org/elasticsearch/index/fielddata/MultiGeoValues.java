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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryTreeReader;

import java.io.IOException;

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

        public GeoPointValue(GeoPoint geoPoint) {
            this.geoPoint = geoPoint;
        }

        public GeoPoint geoPoint() {
            return geoPoint;
        }

        @Override
        public double minLat() {
            return geoPoint.lat();
        }

        @Override
        public double maxLat() {
            return geoPoint.lat();
        }

        @Override
        public double minLon() {
            return geoPoint.lon();
        }

        @Override
        public double maxLon() {
            return geoPoint.lon();
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
        private final double minLat;
        private final double maxLat;
        private final double minLon;
        private final double maxLon;

        public GeoShapeValue(GeometryTreeReader reader) throws IOException {
            Extent extent = reader.getExtent();
            this.minLat = GeoEncodingUtils.decodeLatitude(extent.minY);
            this.maxLat = GeoEncodingUtils.decodeLatitude(extent.maxY);
            this.maxLon = GeoEncodingUtils.decodeLongitude(extent.maxX);
            this.minLon = GeoEncodingUtils.decodeLongitude(extent.minX);
        }

        public GeoShapeValue(double minLat, double minLon, double maxLat, double maxLon) {
            this.minLat = minLat;
            this.minLon = minLon;
            this.maxLat = maxLat;
            this.maxLon = maxLon;
        }

        @Override
        public double minLat() {
            return minLat;
        }

        @Override
        public double maxLat() {
            return maxLat;
        }

        @Override
        public double minLon() {
            return minLon;
        }

        @Override
        public double maxLon() {
            return maxLon;
        }

        @Override
        public double lat() {
            throw new UnsupportedOperationException("centroid of GeoShape is not defined");
        }

        @Override
        public double lon() {
            throw new UnsupportedOperationException("centroid of GeoShape is not defined");
        }
    }

    /**
     * interface for geo-shape and geo-point doc-values to
     * retrieve properties used in aggregations.
     */
    public interface GeoValue {
        double minLat();
        double maxLat();
        double minLon();
        double maxLon();
        double lat();
        double lon();
    }
}
