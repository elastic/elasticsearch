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

package org.elasticsearch.search.lookup.impl;

import com.google.common.collect.ImmutableList;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.lookup.GeoDocValue;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.util.AbstractList;
import java.util.List;


/**
 * Script level doc values, the assumption is that any implementation will implement a <code>getValue</code>
 * and a <code>getValues</code> that return the relevant type that then can be used in scripts.
 */
public interface ScriptDocValuesImpl<T> extends List<T> {

    /**
     * Set the current doc ID.
     */
    void setNextDocId(int docId);

    /**
     * Return a copy of the list of the values for the current document.
     */
    List<T> getValues();

    public final static class Strings extends AbstractList<String> implements ScriptDocValuesImpl<String> {

        private final SortedBinaryDocValues values;

        public Strings(SortedBinaryDocValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            values.setDocument(docId);
        }

        SortedBinaryDocValues getInternalValues() {
            return this.values;
        }

        BytesRef getBytesValue() {
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

        @Override
        public List<String> getValues() {
            return ImmutableList.copyOf(this);
        }

        @Override
        public String get(int index) {
            return values.valueAt(index).utf8ToString();
        }

        @Override
        public int size() {
            return values.count();
        }

    }

    public static class Longs extends AbstractList<Long> implements ScriptDocValuesImpl<Long> {

        private final SortedNumericDocValues values;
        private final MutableDateTime date = new MutableDateTime(0, DateTimeZone.UTC);

        public Longs(SortedNumericDocValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            values.setDocument(docId);
        }

        SortedNumericDocValues getInternalValues() {
            return this.values;
        }

        public long getValue() {
            int numValues = values.count();
            if (numValues == 0) {
                return 0l;
            }
            return values.valueAt(0);
        }

        @Override
        public List<Long> getValues() {
            return ImmutableList.copyOf(this);
        }

        public MutableDateTime getDate() {
            date.setMillis(getValue());
            return date;
        }

        @Override
        public Long get(int index) {
            return values.valueAt(index);
        }

        @Override
        public int size() {
            return values.count();
        }

    }

    public static class Doubles extends AbstractList<Double> implements ScriptDocValuesImpl<Double> {

        private final SortedNumericDoubleValues values;

        public Doubles(SortedNumericDoubleValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            values.setDocument(docId);
        }

        SortedNumericDoubleValues getInternalValues() {
            return this.values;
        }

        public double getValue() {
            int numValues = values.count();
            if (numValues == 0) {
                return 0d;
            }
            return values.valueAt(0);
        }

        @Override
        public List<Double> getValues() {
            return ImmutableList.copyOf(this);
        }

        @Override
        public Double get(int index) {
            return values.valueAt(index);
        }

        @Override
        public int size() {
            return values.count();
        }
    }

    public static class GeoPoints extends AbstractList<GeoDocValue> implements ScriptDocValuesImpl<GeoDocValue> {

        private final MultiGeoPointValues values;

        public GeoPoints(MultiGeoPointValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            values.setDocument(docId);
        }

        public GeoDocValue getValue() {
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
            List<GeoDocValue> points = getValues();
            double[] lats = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            List<GeoDocValue> points = getValues();
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lons[i] = points.get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public List<GeoDocValue> getValues() {
            return ImmutableList.copyOf(this);
        }

        @Override
        public GeoDocValue get(int index) {
            final GeoDocValue point = values.valueAt(index);
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return values.count();
        }

        public double factorDistance(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double factorDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double factorDistance02(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT) + 1;
        }

        public double factorDistance13(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.FACTOR.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT) + 2;
        }

        public double arcDistance(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double arcDistanceInKm(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double arcDistanceInKmWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double arcDistanceInMiles(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double arcDistanceInMilesWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double distance(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double distanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.DEFAULT);
        }

        public double distanceInKm(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double distanceInKmWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.KILOMETERS);
        }

        public double distanceInMiles(double lat, double lon) {
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double distanceInMilesWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            GeoDocValue point = getValue();
            return GeoDistance.PLANE.calculate(point.lat(), point.lon(), lat, lon, DistanceUnit.MILES);
        }

        public double geohashDistance(String geohash) {
            GeoDocValue point = getValue();
            GeoDocValue p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.DEFAULT);
        }

        public double geohashDistanceInKm(String geohash) {
            GeoDocValue point = getValue();
            GeoDocValue p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.KILOMETERS);
        }

        public double geohashDistanceInMiles(String geohash) {
            GeoDocValue point = getValue();
            GeoDocValue p = new GeoPoint().resetFromGeoHash(geohash);
            return GeoDistance.ARC.calculate(point.lat(), point.lon(), p.lat(), p.lon(), DistanceUnit.MILES);
        }

    }
}
