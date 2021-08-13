/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class DateRangeAggregatorFactory extends AbstractRangeAggregatorFactory<RangeAggregator.Range> {
    public DateRangeAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        RangeAggregator.Range[] ranges,
        boolean keyed,
        InternalRange.Factory<?, ?> rangeFactory,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        RangeAggregatorSupplier aggregatorSupplier

    ) throws IOException {
        super(
            name,
            DateRangeAggregationBuilder.REGISTRY_KEY,
            config,
            ranges,
            keyed,
            rangeFactory,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

}
