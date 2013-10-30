/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.util.*;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.SlicedDoubleList;
import org.elasticsearch.common.util.SlicedLongList;
import org.elasticsearch.common.util.SlicedObjectList;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.Collections;
import java.util.List;

/**
 * Script level doc values, the assumption is that any implementation will implement a <code>getValue</code>
 * and a <code>getValues</code> that return the relevant type that then can be used in scripts.
 */
public abstract class ScriptDocValues {

    public static final ScriptDocValues EMPTY = new Empty();
    public static final Strings EMPTY_STRINGS = new Strings(BytesValues.EMPTY);
    protected int docId;
    protected boolean listLoaded = false;

    public void setNextDocId(int docId) {
        this.docId = docId;
        this.listLoaded = false;
    }

    public abstract boolean isEmpty();

    public abstract List<?> getValues();

    public static class Empty extends ScriptDocValues {
        @Override
        public void setNextDocId(int docId) {
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public List<?> getValues() {
            return Collections.emptyList();
        }

    }

    public final static class Strings extends ScriptDocValues {

        private final BytesValues values;
        private final CharsRef spare = new CharsRef();
        private SlicedObjectList<String> list;

        public Strings(BytesValues values) {
            this.values = values;
            list = new SlicedObjectList<String>(values.isMultiValued() ? new String[10] : new String[1]) {

                @Override
                public void grow(int newLength) {
                    assert offset == 0; // NOTE: senseless if offset != 0
                    if (values.length >= newLength) {
                        return;
                    }
                    final String[] current = values;
                    values = new String[ArrayUtil.oversize(newLength, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                    System.arraycopy(current, 0, values, 0, current.length);
                }
            };
        }

        @Override
        public boolean isEmpty() {
            return values.setDocument(docId) == 0;
        }

        public BytesValues getInternalValues() {
            return this.values;
        }

        public BytesRef getBytesValue() {
            int numValues = values.setDocument(docId);
            if (numValues == 0) {
                return null;
            }
            return values.nextValue();
        }

        public String getValue() {
            String value = null;
            if (values.setDocument(docId) > 0) {
                UnicodeUtil.UTF8toUTF16(values.nextValue(), spare);
                value = spare.toString();
            }
            return value;
        }

        public List<String> getValues() {
            if (!listLoaded) {
                final int numValues = values.setDocument(docId);
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    BytesRef next = values.nextValue();
                    UnicodeUtil.UTF8toUTF16(next, spare);
                    list.values[i] = spare.toString();
                }
                listLoaded = true;
            }
            return list;
        }

    }


    public static class Longs extends ScriptDocValues {

        private final LongValues values;
        private final MutableDateTime date = new MutableDateTime(0, DateTimeZone.UTC);
        private final SlicedLongList list;

        public Longs(LongValues values) {
            this.values = values;
            this.list = new SlicedLongList(values.isMultiValued() ? 10 : 1);
        }

        public LongValues getInternalValues() {
            return this.values;
        }

        @Override
        public boolean isEmpty() {
            return values.setDocument(docId) == 0;
        }

        public long getValue() {
            int numValues = values.setDocument(docId);
            if (numValues == 0) {
                return 0l;
            }
            return values.nextValue();
        }

        public List<Long> getValues() {
            if (!listLoaded) {
                final int numValues = values.setDocument(docId);
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    list.values[i] = values.nextValue();
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

        private final DoubleValues values;
        private final SlicedDoubleList list;

        public Doubles(DoubleValues values) {
            this.values = values;
            this.list = new SlicedDoubleList(values.isMultiValued() ? 10 : 1);

        }

        public DoubleValues getInternalValues() {
            return this.values;
        }

        @Override
        public boolean isEmpty() {
            return values.setDocument(docId) == 0;
        }


        public double getValue() {
            int numValues = values.setDocument(docId);
            if (numValues == 0) {
                return 0d;
            }
            return values.nextValue();
        }

        public List<Double> getValues() {
            if (!listLoaded) {
                int numValues = values.setDocument(docId);
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    list.values[i] = values.nextValue();
                }
                listLoaded = true;
            }
            return list;
        }
    }

    public static class GeoPoints extends ScriptDocValues {

        private final GeoPointValues values;
        private final SlicedObjectList<GeoPoint> list;

        public GeoPoints(GeoPointValues values) {
            this.values = values;
            list = new SlicedObjectList<GeoPoint>(values.isMultiValued() ? new GeoPoint[10] : new GeoPoint[1]) {

                @Override
                public void grow(int newLength) {
                    assert offset == 0; // NOTE: senseless if offset != 0
                    if (values.length >= newLength) {
                        return;
                    }
                    final GeoPoint[] current = values;
                    values = new GeoPoint[ArrayUtil.oversize(newLength, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                    System.arraycopy(current, 0, values, 0, current.length);
                }
            };
        }


        @Override
        public boolean isEmpty() {
            return values.setDocument(docId) == 0;
        }

        public GeoPoint getValue() {
            int numValues = values.setDocument(docId);
            if (numValues == 0) {
                return null;
            }
            return values.nextValue();
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
                int numValues = values.setDocument(docId);
                list.offset = 0;
                list.grow(numValues);
                list.length = numValues;
                for (int i = 0; i < numValues; i++) {
                    GeoPoint next = values.nextValue();
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
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double factorDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double factorDistance02(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES) + 1;
        }

        public double factorDistance13(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES) + 2;
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
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

        public double distance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double distanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoPoint point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
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
    }
}
