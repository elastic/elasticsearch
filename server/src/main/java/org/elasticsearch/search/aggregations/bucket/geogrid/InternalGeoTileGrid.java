/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoTileGrid extends InternalGeoGrid<InternalGeoTileGridBucket> {

    InternalGeoTileGrid(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, requiredSize, buckets, metadata);
    }

    public InternalGeoTileGrid(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public InternalGeoGrid<InternalGeoTileGridBucket> create(List<InternalGeoGridBucket> buckets) {
        return new InternalGeoTileGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public InternalGeoGridBucket createBucket(InternalAggregations aggregations, InternalGeoGridBucket prototype) {
        return new InternalGeoTileGridBucket(prototype.hashAsLong, prototype.docCount, aggregations);
    }

    @Override
    InternalGeoGrid<InternalGeoTileGridBucket> create(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new InternalGeoTileGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    InternalGeoTileGridBucket createBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        return new InternalGeoTileGridBucket(hashAsLong, docCount, aggregations);
    }

    @Override
    Reader<InternalGeoTileGridBucket> getBucketReader() {
        return InternalGeoTileGridBucket::new;
    }

    @Override
    public String getWriteableName() {
        return GeoTileGridAggregationBuilder.NAME;
    }
}
