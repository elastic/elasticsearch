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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.SlicedDoubleList;
import org.elasticsearch.common.util.SlicedLongList;
import org.elasticsearch.common.util.SlicedObjectList;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

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
        private SlicedObjectList<String> list;

        public Strings(SortedBinaryDocValues values) {
            this.values = values;
            list = new SlicedObjectList<String>(new String[0]) {

                @Override
                public void grow(int newLength) {
                    assert offset == 0; // NOTE: senseless if offset != 0
                    if (values.length >= newLength) {
                        return;
                    }
                    values = Arrays.copyOf(values, ArrayUtil.oversize(newLength, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                }
            };
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
            if (!listLoaded) {
                values.setDocument(docId);
                final int numValues = values.count();
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    list.values[i] = values.valueAt(i).utf8ToString();
                }
                listLoaded = true;
            }
            return list;
        }

    }

    public static class Longs extends ScriptDocValues {

        private final SortedNumericDocValues values;
        private final MutableDateTime date = new MutableDateTime(0, DateTimeZone.UTC);
        private final SlicedLongList list;

        public Longs(SortedNumericDocValues values) {
            this.values = values;
            this.list = new SlicedLongList(0);
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
            if (!listLoaded) {
                values.setDocument(docId);
                final int numValues = values.count();
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    list.values[i] = values.valueAt(i);
                }
                listLoaded = true;
            }
            return list;
        }

        public MutableDateTime getDate() {
            date.setMillis(getValue());
            return date;
        }

    }

    public static class Doubles extends ScriptDocValues {

        private final SortedNumericDoubleValues values;
        private final SlicedDoubleList list;

        public Doubles(SortedNumericDoubleValues values) {
            this.values = values;
            this.list = new SlicedDoubleList(0);

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
            if (!listLoaded) {
                values.setDocument(docId);
                int numValues = values.count();
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    list.values[i] = values.valueAt(i);
                }
                listLoaded = true;
            }
            return list;
        }
    }

    public static class GeoPoints extends ScriptDocValues {

        private final MultiGeoPointValues values;
        private final SlicedObjectList<GeoPoint> list;

        public GeoPoints(MultiGeoPointValues values) {
            this.values = values;
            list = new SlicedObjectList<GeoPoint>(new GeoPoint[0]) {

                @Override
                public void grow(int newLength) {
                    assert offset == 0; // NOTE: senseless if offset != 0
                    if (values.length >= newLength) {
                        return;
                    }
                    values = Arrays.copyOf(values, ArrayUtil.oversize(newLength, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                }
            };
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
            if (!listLoaded) {
                values.setDocument(docId);
                int numValues = values.count();
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    GeoPoint next = values.valueAt(i);
                    GeoPoint point = list.values[i];
                    if (point == null) {
                        point = list.values[i] = new GeoPoint();
                    }
                    point.reset(next.lat(), next.lon());
                    list.values[i] = point;
                }
                listLoaded = true;
            }
            return list;

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
