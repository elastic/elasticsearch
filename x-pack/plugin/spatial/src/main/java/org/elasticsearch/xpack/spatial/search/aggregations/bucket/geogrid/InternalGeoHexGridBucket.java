/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;

import java.io.IOException;

public class InternalGeoHexGridBucket extends InternalGeoGridBucket {

    InternalGeoHexGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        super(hashAsLong, docCount, aggregations);
    }

    /**
     * Read from a stream.
     */
    public InternalGeoHexGridBucket(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getKeyAsString() {
        return H3.h3ToString(hashAsLong);
    }

    @Override
    public GeoPoint getKey() {
        LatLng latLng = H3.h3ToLatLng(hashAsLong);
        return new GeoPoint(latLng.getLatDeg(), latLng.getLonDeg());
    }
}
