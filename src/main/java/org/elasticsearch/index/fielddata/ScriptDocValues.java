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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.util.*;
import org.joda.time.MutableDateTime;

/**
 * Script level doc values, the assumption is that any implementation will implement a <code>getValue</code>
 * and a <code>getValues</code> that return the relevant type that then can be used in scripts.
 */
public interface ScriptDocValues {

    static final ScriptDocValues EMPTY = new Empty();
    static final Strings EMPTY_STRINGS = new Strings(StringValues.EMPTY);

    void setNextDocId(int docId);

    boolean isEmpty();

    static class Empty implements ScriptDocValues {
        @Override
        public void setNextDocId(int docId) {
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    }

    static class Strings implements ScriptDocValues {

        private final StringValues values;
        private int docId;

        public Strings(StringValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            this.docId = docId;
        }

        @Override
        public boolean isEmpty() {
            return !values.hasValue(docId);
        }

        public String getValue() {
            return values.getValue(docId);
        }

    }



    static class NumericLong implements ScriptDocValues {

        private final LongValues values;
        private final MutableDateTime date = new MutableDateTime(0);
        private int docId;

        public NumericLong(LongValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            this.docId = docId;
        }

        @Override
        public boolean isEmpty() {
            return !values.hasValue(docId);
        }

        public long getValue() {
            return values.getValue(docId);
        }

        public MutableDateTime getDate() {
            date.setMillis(getValue());
            return date;
        }

    }
    static class NumericDouble implements ScriptDocValues {

        private final DoubleValues values;
        private int docId;

        public NumericDouble(DoubleValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            this.docId = docId;
        }

        @Override
        public boolean isEmpty() {
            return !values.hasValue(docId);
        }

        public double getValue() {
            return values.getValue(docId);
        }
    }

    static class GeoPoints implements ScriptDocValues {

        private final GeoPointValues values;
        private int docId;

        public GeoPoints(GeoPointValues values) {
            this.values = values;
        }

        @Override
        public void setNextDocId(int docId) {
            this.docId = docId;
        }

        @Override
        public boolean isEmpty() {
            return !values.hasValue(docId);
        }

        public GeoPoint getValue() {
            return values.getValue(docId);
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
