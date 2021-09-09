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
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

public class GeoShapeTileGridAggregator extends GeoTileGridAggregator {
    public GeoShapeTileGridAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, int requiredSize,
                                      int shardSize, AggregationContext context, Aggregator parent,
                                      CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
        super(name, factories, valuesSource, requiredSize, shardSize, context, parent, cardinality, metadata);
    }

    /**
     * This is a wrapper method to expose this protected method to {@link GeoShapeCellIdSource}
     *
     * @param bytes the number of bytes to register or negative to deregister the bytes
     * @return the cumulative size in bytes allocated by this aggregator to service this request
     */
    public long addRequestBytes(long bytes) {
        return addRequestCircuitBreakerBytes(bytes);
    }
}
