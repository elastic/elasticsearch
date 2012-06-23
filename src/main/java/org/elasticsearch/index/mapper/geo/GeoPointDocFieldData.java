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

package org.elasticsearch.index.mapper.geo;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.field.data.DocFieldData;

/**
 *
 */
public class GeoPointDocFieldData extends DocFieldData<GeoPointFieldData> {

    public GeoPointDocFieldData(GeoPointFieldData fieldData) {
        super(fieldData);
    }

    public GeoPoint getValue() {
        return fieldData.value(docId);
    }

    public GeoPoint[] getValues() {
        return fieldData.values(docId);
    }

    public double factorDistance(double lat, double lon) {
        return fieldData.factorDistance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double factorDistanceWithDefault(double lat, double lon, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.factorDistance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double factorDistance02(double lat, double lon) {
        return fieldData.factorDistance(docId, DistanceUnit.MILES, lat, lon) + 1;
    }

    public double factorDistance13(double lat, double lon) {
        return fieldData.factorDistance(docId, DistanceUnit.MILES, lat, lon) + 2;
    }

    public double arcDistance(double lat, double lon) {
        return fieldData.arcDistance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.arcDistance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double arcDistanceInKm(double lat, double lon) {
        return fieldData.arcDistance(docId, DistanceUnit.KILOMETERS, lat, lon);
    }

    public double arcDistanceInKmWithDefault(double lat, double lon, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.arcDistance(docId, DistanceUnit.KILOMETERS, lat, lon);
    }

    public double distance(double lat, double lon) {
        return fieldData.distance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double distanceWithDefault(double lat, double lon, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.distance(docId, DistanceUnit.MILES, lat, lon);
    }

    public double distanceInKm(double lat, double lon) {
        return fieldData.distance(docId, DistanceUnit.KILOMETERS, lat, lon);
    }

    public double distanceInKmWithDefault(double lat, double lon, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.distance(docId, DistanceUnit.KILOMETERS, lat, lon);
    }

    public double geohashDistance(String geohash) {
        return fieldData.distanceGeohash(docId, DistanceUnit.MILES, geohash);
    }

    public double geohashDistanceWithDefault(String geohash, double defaultValue) {
        if (!fieldData.hasValue(docId)) {
            return defaultValue;
        }
        return fieldData.distanceGeohash(docId, DistanceUnit.MILES, geohash);
    }

    public double geohashDistanceInKm(String geohash) {
        return fieldData.distanceGeohash(docId, DistanceUnit.KILOMETERS, geohash);
    }

    public double getLat() {
        return fieldData.latValue(docId);
    }

    public double getLon() {
        return fieldData.lonValue(docId);
    }

    public double[] getLats() {
        return fieldData.latValues(docId);
    }

    public double[] getLons() {
        return fieldData.lonValues(docId);
    }
}
