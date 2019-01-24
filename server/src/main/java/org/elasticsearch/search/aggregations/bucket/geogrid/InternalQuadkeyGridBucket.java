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

import org.elasticsearch.common.geo.QuadkeyUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;

public class InternalQuadkeyGridBucket extends InternalGeoGridBucket<InternalQuadkeyGridBucket> {
    InternalQuadkeyGridBucket(long geohashAsLong, long docCount, InternalAggregations aggregations) {
        super(geohashAsLong, docCount, aggregations);
    }

    /**
     * Read from a stream.
     */
    public InternalQuadkeyGridBucket(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    InternalQuadkeyGridBucket buildBucket(InternalGeoGridBucket bucket, long geoHashAsLong, long docCount,
                                          InternalAggregations aggregations) {
        return new InternalQuadkeyGridBucket(geoHashAsLong, docCount, aggregations);
    }

    @Override
    public String getKeyAsString() {
        return QuadkeyUtils.stringEncode(geohashAsLong);
    }

    @Override
    public GeoPoint getKey() {
        return GeoPoint.fromGeohash(geohashAsLong);
    }
}
