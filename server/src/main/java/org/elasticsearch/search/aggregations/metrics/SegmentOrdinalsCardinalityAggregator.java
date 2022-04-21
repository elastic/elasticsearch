/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * This aggregator uses a heuristic to decide between direct collection and using segment ordinals, based on the expected memory overhead
 * of the ordinals approach.
 */
public class SegmentOrdinalsCardinalityAggregator extends CardinalityAggregator {

    private final ValuesSource.Bytes.WithOrdinals source;

    public SegmentOrdinalsCardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata);
        source = (ValuesSource.Bytes.WithOrdinals) valuesSourceConfig.getValuesSource();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();
        final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
        final long maxOrd = ordinalValues.getValueCount();
        if (maxOrd == 0) {
            emptyCollectorsUsed++;
            return new EmptyCollector();
        }

        final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
        final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
        // only use ordinals if they don't increase memory usage by more than 25%
        if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
            ordinalsCollectorsUsed++;
            return new OrdinalsCollector(counts, ordinalValues, bigArrays());
        }
        ordinalsCollectorsOverheadTooHigh++;
        stringHashingCollectorsUsed++;
        return new DirectCollector(counts, MurmurHash3Values.hash(source.bytesValues(ctx)));
    }
}
