/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface GeoGridAggregatorSupplier {
    GeoGridAggregator<? extends InternalGeoGrid<?>> build(
        String name,
        AggregatorFactories factories,
        ValuesSource valuesSource,
        int precision,
        GeoBoundingBox geoBoundingBox,
        int requiredSize,
        int shardSize,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;
}
