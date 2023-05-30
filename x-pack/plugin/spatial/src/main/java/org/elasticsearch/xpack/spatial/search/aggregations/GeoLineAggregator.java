/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.unboundid.util.NotNull;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
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
abstract class GeoLineAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    protected final GeoLineMultiValuesSource valuesSources;

    protected final boolean includeSorts;
    protected final SortOrder sortOrder;
    protected final int size;

    private GeoLineAggregator(
        String name,
        GeoLineMultiValuesSource valuesSources,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metaData,
        boolean includeSorts,
        SortOrder sortOrder,
        int size
    ) throws IOException {
        super(name, context, parent, metaData);
        this.valuesSources = valuesSources;
        this.includeSorts = includeSorts;
        this.sortOrder = sortOrder;
        this.size = size;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSources.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoLine(name, new long[0], new double[0], metadata(), true, includeSorts, sortOrder, size);
    }

    static class Empty extends GeoLineAggregator {
        Empty(
            String name,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metaData,
            boolean includeSorts,
            SortOrder sortOrder,
            int size
        ) throws IOException {
            super(name, null, context, parent, metaData, includeSorts, sortOrder, size);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public InternalAggregation buildAggregation(long bucket) {
            return buildEmptyAggregation();
        }
    }

    static class Normal extends GeoLineAggregator {
        protected final BigArrays bigArrays;
        protected LongArray counts;
        private final GeoLineBucketedSort sort;
        private final GeoLineBucketedSort.Extra extra;

        Normal(
            String name,
            @NotNull GeoLineMultiValuesSource valuesSources,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metaData,
            boolean includeSorts,
            SortOrder sortOrder,
            int size
        ) throws IOException {
            super(name, valuesSources, context, parent, metaData, includeSorts, sortOrder, size);
            this.bigArrays = context.bigArrays();
            this.extra = new GeoLineBucketedSort.Extra(bigArrays, valuesSources);
            this.sort = new GeoLineBucketedSort(bigArrays, sortOrder, null, size, valuesSources, extra);
            this.counts = bigArrays.newLongArray(1, true);
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
            BucketedSort.Leaf leafSort = sort.forLeaf(aggCtx.getLeafReaderContext());

            return new LeafBucketCollector() {
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
            if (bucket >= counts.size()) {
                return buildEmptyAggregation();
            }
            boolean complete = counts.get(bucket) <= size;
            return sort.buildAggregation(bucket, name, metadata(), complete, includeSorts, size, this::addRequestCircuitBreakerBytes);
        }

        @Override
        public void doClose() {
            super.doClose();
            Releasables.close(counts, sort, extra);
        }
    }

    static class TimeSeries extends GeoLineAggregator {
        private final TimeSeriesGeoLineBuckets geolineBuckets;

        TimeSeries(
            String name,
            @NotNull GeoLineMultiValuesSource valuesSources,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metaData,
            boolean includeSorts,
            SortOrder sortOrder,
            int size
        ) throws IOException {
            super(name, valuesSources, context, parent, metaData, includeSorts, sortOrder, size);
            this.geolineBuckets = new TimeSeriesGeoLineBuckets(
                size,
                valuesSources,
                this::buildGeolineForBucket,
                this::addRequestCircuitBreakerBytes
            );
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
            TimeSeriesGeoLineBuckets.Leaf leaf = geolineBuckets.forLeaf(aggCtx);

            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    leaf.collect(doc, bucket);
                }
            };
        }

        private InternalGeoLine buildGeolineForBucket(long bucket) {
            // The time-series geo_line does not need to know about sortOrder or other fields until creation of the InternalGeoLine
            return geolineBuckets.buildAggregation(bucket, name, metadata(), includeSorts, sortOrder);
        }

        @Override
        public InternalAggregation buildAggregation(long bucket) {
            return geolineBuckets.getGeolineForBucket(bucket);
        }

        @Override
        protected void doPostCollection() {
            // Ensure any last bucket is completed
            geolineBuckets.doPostCollection();
        }

        @Override
        public void doClose() {
            super.doClose();
            geolineBuckets.doClose();
        }
    }
}
