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

package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;

/**
 * A simple wrapper for GeoUtils handling of the geohash hashing algorithm
 */
public class GeoHashType implements GeoGridType {

    /**
     * GeoGridType must be a singleton because bucket does a reference compare for equality
     */
    private GeoHashType() {}

    public static final String NAME = "geohash";

    public static final GeoGridType SINGLETON = new GeoHashType();

    @Override
    public String getName() {
        return NAME;
    }

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
    public GeoGridBucket createBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        return new GeoHashBucket(hashAsLong, docCount, aggregations);
    }

    @Override
    public GeoGridBucket createBucket(StreamInput reader) throws IOException {
        return new GeoHashBucket(reader);
    }
}
