/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

public interface StringStatsAggregatorSupplier {

    Aggregator build(
        String name,
        ValuesSource valuesSource,
        boolean showDistribution,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException;
}
