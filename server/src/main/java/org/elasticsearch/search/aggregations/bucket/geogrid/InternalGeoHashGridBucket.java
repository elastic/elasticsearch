/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;

public class InternalGeoHashGridBucket extends InternalGeoGridBucket {
    InternalGeoHashGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        super(hashAsLong, docCount, aggregations);
    }

    /**
     * Read from a stream.
     */
    public InternalGeoHashGridBucket(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getKeyAsString() {
        return Geohash.stringEncode(hashAsLong);
    }

    @Override
    public GeoPoint getKey() {
        return GeoPoint.fromGeohash(hashAsLong);
    }
}
