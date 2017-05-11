/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics.stats;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StatsAggregator extends NumericMetricsAggregator.MultiValue {

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;

    LongArray counts;
    DoubleArray sums;
    DoubleArray mins;
    DoubleArray maxes;


    public StatsAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat format,
            SearchContext context,
                           Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                           Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            counts = bigArrays.newLongArray(1, true);
            sums = bigArrays.newDoubleArray(1, true);
            mins = bigArrays.newDoubleArray(1, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
            maxes = bigArrays.newDoubleArray(1, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        }
        this.format = format;
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= counts.size()) {
                    final long from = counts.size();
                    final long overSize = BigArrays.overSize(bucket + 1);
                    counts = bigArrays.resize(counts, overSize);
                    sums = bigArrays.resize(sums, overSize);
                    mins = bigArrays.resize(mins, overSize);
                    maxes = bigArrays.resize(maxes, overSize);
                    mins.fill(from, overSize, Double.POSITIVE_INFINITY);
                    maxes.fill(from, overSize, Double.NEGATIVE_INFINITY);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    counts.increment(bucket, valuesCount);
                    double sum = 0;
                    double min = mins.get(bucket);
                    double max = maxes.get(bucket);
                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        sum += value;
                        min = Math.min(min, value);
                        max = Math.max(max, value);
                    }
                    sums.increment(bucket, sum);
                    mins.set(bucket, min);
                    maxes.set(bucket, max);
                }
            }
        };
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalStats.Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= counts.size()) {
            switch(InternalStats.Metrics.resolve(name)) {
                case count: return 0;
                case sum: return 0;
                case min: return Double.POSITIVE_INFINITY;
                case max: return Double.NEGATIVE_INFINITY;
                case avg: return Double.NaN;
                default:
                    throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
            }
        }
        switch(InternalStats.Metrics.resolve(name)) {
            case count: return counts.get(owningBucketOrd);
            case sum: return sums.get(owningBucketOrd);
            case min: return mins.get(owningBucketOrd);
            case max: return maxes.get(owningBucketOrd);
            case avg: return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalStats(name, counts.get(bucket), sums.get(bucket), mins.get(bucket),
                maxes.get(bucket), format, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalStats(name, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, maxes, mins, sums);
    }
}
