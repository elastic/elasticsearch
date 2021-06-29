/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoLineMultiValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Metric Aggregation for joining sorted geo_point values into a single path
 **/
final class GeoLineAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    private final GeoLineMultiValuesSource valuesSources;

    private final BigArrays bigArrays;
    private final GeoLineBucketedSort sort;
    private final GeoLineBucketedSort.Extra extra;
    private LongArray counts;
    private final boolean includeSorts;
    private final SortOrder sortOrder;
    private final int size;

    GeoLineAggregator(String name, GeoLineMultiValuesSource valuesSources, AggregationContext context,
                      Aggregator parent, Map<String,Object> metaData, boolean includeSorts, SortOrder sortOrder,
                      int size) throws IOException {
        super(name, context, parent, metaData);
        this.valuesSources = valuesSources;
        this.bigArrays = context.bigArrays();
        if (valuesSources != null) {
            this.extra = new GeoLineBucketedSort.Extra(bigArrays, valuesSources);
            this.sort = new GeoLineBucketedSort(bigArrays, sortOrder, null, size, valuesSources, extra);
            this.counts = bigArrays.newLongArray(1, true);
        } else {
            this.extra = null;
            this.sort = null;
        }
        this.includeSorts = includeSorts;
        this.sortOrder = sortOrder;
        this.size = size;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSources != null && valuesSources.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        BucketedSort.Leaf leafSort = sort.forLeaf(ctx);

        return new LeafBucketCollector(){
            @Override
            public void collect(int doc, long bucket) throws IOException {
                leafSort.collect(doc, bucket);
                counts = bigArrays.grow(counts, bucket + 1);
                counts.increment(bucket, 1);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null) {
            return buildEmptyAggregation();
        }
        boolean complete = counts.get(bucket) <= size;
        addRequestCircuitBreakerBytes((Double.SIZE + Long.SIZE) * sort.sizeOf(bucket));
        double[] sortVals = sort.getSortValues(bucket);
        long[] bucketLine = sort.getPoints(bucket);
        new PathArraySorter(bucketLine, sortVals, sortOrder).sort();
        return new InternalGeoLine(name, bucketLine, sortVals, metadata(), complete, includeSorts, sortOrder, size);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoLine(name, null, null, metadata(), true, includeSorts, sortOrder, size);
    }

    @Override
    public void doClose() {
        Releasables.close(sort, extra, counts);
    }
}
