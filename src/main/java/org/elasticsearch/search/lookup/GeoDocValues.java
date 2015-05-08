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

package org.elasticsearch.search.lookup;

public interface GeoDocValues extends ScriptDocValues<GeoDocValue> {
    
    GeoDocValue getValue();

    double getLat();
    
    double getLon();

    double[] getLats();

    double[] getLons();

    double factorDistance(double lat, double lon);

    double factorDistanceWithDefault(double lat, double lon, double defaultValue);

    double factorDistance02(double lat, double lon);

    double factorDistance13(double lat, double lon);

    double arcDistance(double lat, double lon);

    double arcDistanceWithDefault(double lat, double lon, double defaultValue);

    double arcDistanceInKm(double lat, double lon);

    double arcDistanceInKmWithDefault(double lat, double lon, double defaultValue);

    double arcDistanceInMiles(double lat, double lon);

    double arcDistanceInMilesWithDefault(double lat, double lon, double defaultValue);

    double distance(double lat, double lon);

    double distanceWithDefault(double lat, double lon, double defaultValue);

    double distanceInKm(double lat, double lon);

    double distanceInKmWithDefault(double lat, double lon, double defaultValue);

    double distanceInMiles(double lat, double lon);

    double distanceInMilesWithDefault(double lat, double lon, double defaultValue);

    double geohashDistance(String geohash);

    double geohashDistanceInKm(String geohash);

    double geohashDistanceInMiles(String geohash);
}
