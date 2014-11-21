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

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;


/**
 * Script level doc values, the assumption is that any implementation will implement a <code>getValue</code>
 * and a <code>getValues</code> that return the relevant type that then can be used in scripts.
 */
public abstract class ScriptDocValues {

    protected int docId;
    protected boolean listLoaded = false;

    public void setNextDocId(int docId) {
        this.docId = docId;
        this.listLoaded = false;
    }

    public abstract boolean isEmpty();

    public abstract List<?> getValues();

    public final static class Strings extends ScriptDocValues {

        private final SortedBinaryDocValues values;

        public Strings(SortedBinaryDocValues values) {
            this.values = values;
        }

        @Override
        public boolean isEmpty() {
            values.setDocument(docId);
            return values.count() == 0;
        }

        public SortedBinaryDocValues getInternalValues() {
            return this.values;
        }

        public BytesRef getBytesValue() {
            values.setDocument(docId);
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return null;
            }
        }

        public String getValue() {
            BytesRef value = getBytesValue();
            if (value == null) {
                return null;
            } else {
                return value.utf8ToString();
            }
        }

        public List<String> getValues() {
            values.setDocument(docId);
            final int valueCount = values.count();
            final String[] vals = new String[valueCount];
            for (int i = 0; i < valueCount; ++i) {
                vals[i] = values.valueAt(i).utf8ToString();
            }
            return Arrays.asList(vals);
        }

    }

    public static class Longs extends ScriptDocValues {

        private final SortedNumericDocValues values;
        private final MutableDateTime date = new MutableDateTime(0, DateTimeZone.UTC);

        public Longs(SortedNumericDocValues values) {
            this.values = values;
        }

        public SortedNumericDocValues getInternalValues() {
            return this.values;
        }

        @Override
        public boolean isEmpty() {
            values.setDocument(docId);
            return values.count() == 0;
        }

        public long getValue() {
            values.setDocument(docId);
            int numValues = values.count();
            if (numValues == 0) {
                return 0l;
            }
            return values.valueAt(0);
        }

        public List<Long> getValues() {
            values.setDocument(docId);
            final int valueCount = values.count();
            final long[] vals = new long[valueCount];
            for (int i = 0; i < valueCount; ++i) {
                vals[i] = values.valueAt(i);
            }
            return new AbstractList<Long>() {
                @Override
                public Long get(int i) {
                    return vals[i];
                }
                @Override
                public int size() {
                    return valueCount;
                }
            };
        }

        public MutableDateTime getDate() {
            date.setMillis(getValue());
            return date;
        }

    }

    public static class Doubles extends ScriptDocValues {

        private final SortedNumericDoubleValues values;

        public Doubles(SortedNumericDoubleValues values) {
            this.values = values;

        }

        public SortedNumericDoubleValues getInternalValues() {
            return this.values;
        }

        @Override
        public boolean isEmpty() {
            values.setDocument(docId);
            return values.count() == 0;
        }


        public double getValue() {
            values.setDocument(docId);
            int numValues = values.count();
            if (numValues == 0) {
                return 0d;
            }
            return values.valueAt(0);
        }

        public List<Double> getValues() {
            values.setDocument(docId);
            final int valueCount = values.count();
            final double[] vals = new double[valueCount];
            for (int i = 0; i < valueCount; ++i) {
                vals[i] = values.valueAt(i);
            }
            return new AbstractList<Double>() {
                @Override
                public Double get(int i) {
                    return vals[i];
                }
                @Override
                public int size() {
                    return valueCount;
                }
            };
        }
    }

    public static class GeoPoints extends ScriptDocValues {

        private final MultiGeoPointValues values;

        public GeoPoints(MultiGeoPointValues values) {
            this.values = values;
        }

        @Override
        public boolean isEmpty() {
            values.setDocument(docId);
            return values.count() == 0;
        }

        public GeoPoint getValue() {
            values.setDocument(docId);
            int numValues = values.count();
            if (numValues == 0) {
                return null;
            }
            return values.valueAt(0);
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            List<GeoPoint> points = getValues();
            double[] lats = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            List<GeoPoint> points = getValues();
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lons[i] = points.get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        public List<GeoPoint> getValues() {
            values.setDocument(docId);
            final int valueCount = values.count();
            final GeoPoint[] vals = new GeoPoint[valueCount];
            for (int i = 0; i < valueCount; ++i) {
                final GeoPoint point = values.valueAt(i);
                vals[i] = new GeoPoint(point.lat(), point.lon());
            }
            return Arrays.asList(vals);

        }

        public double factorDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double factorDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double factorDistance02(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT) + 1;
        }

        public double factorDistance13(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT) + 2;
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double arcDistanceInKm(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double arcDistanceInKmWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double arcDistanceInMiles(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double arcDistanceInMilesWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double distance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double distanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double distanceInKm(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double distanceInKmWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double distanceInMiles(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double distanceInMilesWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            GeoPoint p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.DEFAULT);
        }

        public double geohashDistanceInKm(String geohash) {
            GeoPoint point = getValue();
            GeoPoint p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.KILOMETERS);
        }

        public double geohashDistanceInMiles(String geohash) {
            GeoPoint point = getValue();
            GeoPoint p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.MILES);
        }

    }
}
