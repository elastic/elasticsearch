/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;
import java.util.Map;

public class GeoTileGridTests extends GeoGridTestCase<InternalGeoTileGridBucket, InternalGeoTileGrid> {

    @Override
    protected InternalGeoTileGrid createInternalGeoGrid(
        String name,
        int size,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
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
