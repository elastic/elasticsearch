/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as geotile longs (for efficiency's sake) but formats results as geotile strings.
 */
public class GeoTileGridAggregator extends GeoGridAggregator<InternalGeoTileGrid> {

    public GeoTileGridAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        int requiredSize,
        int shardSize,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, valuesSource, requiredSize, shardSize, context, parent, cardinality, metadata);
    }

    @Override
    protected InternalGeoTileGrid buildAggregation(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new InternalGeoTileGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public InternalGeoTileGrid buildEmptyAggregation() {
        return new InternalGeoTileGrid(name, requiredSize, Collections.emptyList(), metadata());
    }

    protected InternalGeoGridBucket newEmptyBucket() {
        return new InternalGeoTileGridBucket(0, 0, null);
    }
}
