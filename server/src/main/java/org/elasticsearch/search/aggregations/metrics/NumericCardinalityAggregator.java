/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * Specialization of the cardinality aggregator to collect numeric values.
 */
public class NumericCardinalityAggregator extends CardinalityAggregator {

    private final ValuesSource.Numeric source;

    public NumericCardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata);
        this.source = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();
        MurmurHash3Values hashValues = source.isFloatingPoint()
            ? MurmurHash3Values.hash(source.doubleValues(ctx))
            : MurmurHash3Values.hash(source.longValues(ctx));
        return new DirectCollector(counts, hashValues);
    }
}
