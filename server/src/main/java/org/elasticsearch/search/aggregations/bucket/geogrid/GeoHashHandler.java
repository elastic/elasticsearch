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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;

public class GeoHashHandler implements GeoHashTypeProvider {
    @Override
    public int getDefaultPrecision() {
        return 5;
    }

    @Override
    public int parsePrecisionString(String precision) {
        return GeoUtils.parsePrecisionString(precision);
    }

    @Override
    public int validatePrecision(int precision) {
        return GeoUtils.checkPrecisionRange(precision);
    }

    @Override
    public long calculateHash(double longitude, double latitude, int precision) {
        return GeoHashUtils.longEncode(longitude, latitude, precision);
    }

    @Override
    public String hashAsString(long hash) {
        return GeoHashUtils.stringEncode(hash);
    }

    @Override
    public GeoPoint hashAsObject(long hash) {
        return GeoPoint.fromGeohash(hash);
    }
}
