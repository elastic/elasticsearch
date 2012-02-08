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

package org.elasticsearch.index.search.geo;

/**
 */
public class GeoUtils {

    public static double normalizeLon(double lon) {
        return centeredModulus(lon, 360);
    }

    public static double normalizeLat(double lat) {
        return centeredModulus(lat, 180);
    }

    private static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0)
            rtn += divisor;
        if (rtn > divisor/2)
            rtn -= divisor;
        return rtn;
    }

}
