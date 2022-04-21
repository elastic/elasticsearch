/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents a grid of cells where each cell's location is determined by a h3 cell.
 * All cells in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoHexGrid extends InternalGeoGrid<InternalGeoHexGridBucket> {

    InternalGeoHexGrid(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, requiredSize, buckets, metadata);
    }

    public InternalGeoHexGrid(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public InternalGeoGrid<InternalGeoHexGridBucket> create(List<InternalGeoGridBucket> buckets) {
        return new InternalGeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public InternalGeoGridBucket createBucket(InternalAggregations aggregations, InternalGeoGridBucket prototype) {
        return new InternalGeoHexGridBucket(prototype.hashAsLong(), prototype.getDocCount(), aggregations);
    }

    @Override
    protected InternalGeoGrid<InternalGeoHexGridBucket> create(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new InternalGeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    protected InternalGeoHexGridBucket createBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        return new InternalGeoHexGridBucket(hashAsLong, docCount, aggregations);
    }

    @Override
    protected Reader<InternalGeoHexGridBucket> getBucketReader() {
        return InternalGeoHexGridBucket::new;
    }

    @Override
    public String getWriteableName() {
        return GeoHexGridAggregationBuilder.NAME;
    }
}
