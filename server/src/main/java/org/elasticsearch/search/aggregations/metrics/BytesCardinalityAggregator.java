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
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * Cardinality aggregator that operates directly on the bytes values of a field.  This is necessary for fields that don't use ordinals at
 * all, such as Range Fields.
 */
public class BytesCardinalityAggregator extends CardinalityAggregator {

    public BytesCardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();
        stringHashingCollectorsUsed++;
        return new DirectCollector(counts, MurmurHash3Values.hash(valuesSourceConfig.getValuesSource().bytesValues(ctx)));
    }
}
