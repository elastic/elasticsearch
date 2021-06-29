/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface DateHistogramAggregationSupplier {
    Aggregator build(String name,
                     AggregatorFactories factories,
                     Rounding rounding,
                     BucketOrder order,
                     boolean keyed,
                     long minDocCount,
                     @Nullable LongBounds extendedBounds,
                     @Nullable LongBounds hardBounds,
                     ValuesSourceConfig valuesSourceConfig,
                     AggregationContext context,
                     Aggregator parent,
                     CardinalityUpperBound cardinality,
                     Map<String, Object> metadata) throws IOException;
}
