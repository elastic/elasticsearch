/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongConsumer;

/**
 * Aggregates data expressed as h3 longs (for efficiency's sake)
 * but formats results as h3 strings.
 */
public class GeoHexGridAggregator extends GeoGridAggregator<InternalGeoHexGrid> {

    public GeoHexGridAggregator(
        String name,
        AggregatorFactories factories,
        Function<LongConsumer, ValuesSource.Numeric> valuesSource,
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
    protected InternalGeoHexGrid buildAggregation(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new InternalGeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public InternalGeoHexGrid buildEmptyAggregation() {
        return new InternalGeoHexGrid(name, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    protected InternalGeoGridBucket newEmptyBucket() {
        return new InternalGeoHexGridBucket(0, 0, null);
    }
}
