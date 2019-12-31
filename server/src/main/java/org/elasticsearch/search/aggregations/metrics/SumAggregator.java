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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class SumAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private DoubleArray doubleSums;
    private LongArray longSums;
    private DoubleArray compensations;

    SumAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat formatter, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.format = formatter;
        if (valuesSource != null) {
            doubleSums = context.bigArrays().newDoubleArray(1, true);
            longSums = context.bigArrays().newLongArray(1, true);
            compensations = context.bigArrays().newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        if (valuesSource.isFloatingPoint()) {
            final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
            final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    doubleSums = bigArrays.grow(doubleSums, bucket + 1);
                    compensations = bigArrays.grow(compensations, bucket + 1);

                    if (values.advanceExact(doc)) {
                        final int valuesCount = values.docValueCount();
                        // Compute the sum of double values with Kahan summation algorithm which is more
                        // accurate than naive summation.
                        double sum = doubleSums.get(bucket);
                        double compensation = compensations.get(bucket);
                        kahanSummation.reset(sum, compensation);

                        for (int i = 0; i < valuesCount; i++) {
                            double value = values.nextValue();
                            kahanSummation.add(value);
                        }

                        compensations.set(bucket, kahanSummation.delta());
                        doubleSums.set(bucket, kahanSummation.value());
                    }
                }
            };
        } else {
            final SortedNumericDocValues values = valuesSource.longValues(ctx);
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    longSums = bigArrays.grow(longSums, bucket + 1);

                    if (values.advanceExact(doc)) {
                        final int valuesCount = values.docValueCount();
                        // Compute the sum of long values with naive summation.
                        long sum = longSums.get(bucket);

                        for (int i = 0; i < valuesCount; i++) {
                            long value = values.nextValue();
                            sum += value;
                        }

                        longSums.set(bucket, sum);
                    }
                }
            };
        }
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= doubleSums.size() || owningBucketOrd >= longSums.size()) {
            return 0.0;
        }
        if (valuesSource.isFloatingPoint()) {
            return doubleSums.get(owningBucketOrd);
        } else {
            return (double) longSums.get(owningBucketOrd);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= doubleSums.size() || bucket >= longSums.size()) {
            return buildEmptyAggregation();
        }
        if (valuesSource.isFloatingPoint()) {
            return new InternalSum(name, doubleSums.get(bucket), format, pipelineAggregators(), metaData());
        } else {
            return new InternalSum(name, longSums.get(bucket), format, pipelineAggregators(), metaData());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(doubleSums, longSums, compensations);
    }
}
