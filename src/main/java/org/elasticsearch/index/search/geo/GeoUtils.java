/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.search.geo;

/**
 */
public class GeoUtils {

    public static double normalizeLon(double lon) {
        double delta = 0;
        if (lon < 0) {
            delta = 360;
        } else if (lon >= 0) {
            delta = -360;
        }

        double newLng = lon;
        while (newLng < -180 || newLng > 180) {
            newLng += delta;
        }
        return newLng;
    }

    public static double normalizeLat(double lat) {
        double delta = 0;
        if (lat < 0) {
            delta = 180;
        } else if (lat >= 0) {
            delta = -180;
        }

        double newLat = lat;
        while (newLat < -90 || newLat > 90) {
            newLat += delta;
        }
        return newLat;
    }
}
