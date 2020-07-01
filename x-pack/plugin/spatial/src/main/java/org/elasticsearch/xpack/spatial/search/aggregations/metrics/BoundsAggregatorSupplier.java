/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface BoundsAggregatorSupplier extends AggregatorSupplier {

    MetricsAggregator build(
        String name,
        SearchContext aggregationContext,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException;
}
