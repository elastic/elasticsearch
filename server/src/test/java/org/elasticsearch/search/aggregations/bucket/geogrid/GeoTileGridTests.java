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

import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;
import java.util.Map;

public class GeoTileGridTests extends GeoGridTestCase<InternalGeoTileGridBucket, InternalGeoTileGrid> {

    @Override
    protected InternalGeoTileGrid createInternalGeoGrid(String name, int size, List<InternalGeoGridBucket> buckets,
                                                        Map<String, Object> metadata) {
        return new InternalGeoTileGrid(name, size, buckets, metadata);
    }

    @Override
    protected InternalGeoTileGridBucket createInternalGeoGridBucket(Long key, long docCount, InternalAggregations aggregations) {
        return new InternalGeoTileGridBucket(key, docCount, aggregations);
    }

    @Override
    protected long longEncode(double lng, double lat, int precision) {
        return GeoTileUtils.longEncode(lng, lat, precision);
    }

    @Override
    protected int randomPrecision() {
        // precision values below 8 can lead to parsing errors
        return randomIntBetween(8, GeoTileUtils.MAX_ZOOM);
    }
}
