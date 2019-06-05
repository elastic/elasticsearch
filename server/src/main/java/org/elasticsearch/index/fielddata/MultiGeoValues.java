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

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryTreeReader;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link GeoPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   GeoPointValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoPoint value = values.valueAt(i);
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
        private final GeometryTreeReader reader;

        public GeoShapeValue(GeometryTreeReader reader) {
            this.reader = reader;
        }

        @Override
        public double minLat() {
            return 0;
        }

        @Override
        public double maxLat() {
            return 0;
        }

        @Override
        public double minLon() {
            return 0;
        }

        @Override
        public double maxLon() {
            return 0;
        }

        @Override
        public double lat() {
            return 0;
        }

        @Override
        public double lon() {
            return 0;
        }

        @Override
        public String toString() {
            return "";
        }
    }

    public interface GeoValue {
        double minLat();
        double maxLat();
        double minLon();
        double maxLon();
        double lat();
        double lon();
    }
}
