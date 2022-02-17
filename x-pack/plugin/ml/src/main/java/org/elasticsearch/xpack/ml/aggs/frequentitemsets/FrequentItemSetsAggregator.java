/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReduceAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class FrequentItemSetsAggregator extends MapReduceAggregator {

    protected FrequentItemSetsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        List<ValuesSourceConfig> configs,
        double minimumSupport,
        int minimumSetSize,
        int size
    ) throws IOException {
        super(
            name,
            context,
            parent,
            metadata,
            new AprioriMapReducer(FrequentItemSetsAggregationBuilder.NAME, minimumSupport, minimumSetSize, size),
            configs
        );
    }

}
