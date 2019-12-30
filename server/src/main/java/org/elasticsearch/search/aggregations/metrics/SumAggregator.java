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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
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

    private static final Logger LOG = LogManager.getLogger(SumAggregator.class);

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private LongArray sumsLong;
    private DoubleArray compensations;

    SumAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat formatter, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.format = formatter;
        if (valuesSource != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
            sumsLong = context.bigArrays().newLongArray(1, true);
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
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
                sumsLong = bigArrays.grow(sumsLong, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    if (valuesSource.isFloatingPoint()) {
                        // Compute the sum of double values with Kahan summation algorithm which is more
                        // accurate than naive summation.
                        double sum = sums.get(bucket);
                        double compensation = compensations.get(bucket);
                        kahanSummation.reset(sum, compensation);
                        LOG.info("floating sums get sum: {}, compensation: {}", sum, compensation);

                        for (int i = 0; i < valuesCount; i++) {
                            double value = values.nextValue();
                            kahanSummation.add(value);
                        }

                        compensations.set(bucket, kahanSummation.delta());
                        sums.set(bucket, kahanSummation.value());
                    } else {
                        // Compute the sum of long values with naive summation.
                        long sum = sumsLong.get(bucket);
                        LOG.info("long sums get sum: {}, long sum: {}", sumsLong.get(bucket), sum);
                        for (int i = 0; i < valuesCount; i++) {
                            double doubleValue = values.nextValue();
                            long value = (long) doubleValue;
                            sum += value;
                            LOG.info("summing... doubleValue: {}, value: {}, sum: {}", doubleValue, value, sum);
                        }
                        sumsLong.set(bucket, sum);
//                        long checkSum = sumsLong.get(bucket);
//                        if (checkSum != sum) {
//                            LOG.info("Parsing type warning, sum: {}, checkSum: {}", sum, checkSum);
//                        }
                    }
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size() || owningBucketOrd >= sumsLong.size()) {
            return 0.0;
        }
        if (valuesSource.isFloatingPoint()) {
            LOG.info("get metric double sum = {}", sums.get(owningBucketOrd));
            return sums.get(owningBucketOrd);
        } else {
            LOG.info("get metric long sum = {}", sumsLong.get(owningBucketOrd));
            return sumsLong.get(owningBucketOrd);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size() || bucket >= sumsLong.size()) {
            return buildEmptyAggregation();
        }
        if (valuesSource.isFloatingPoint()) {
            LOG.info("build agg double sum = {}", sums.get(bucket));
            return new InternalSum(name, sums.get(bucket), format, pipelineAggregators(), metaData());
        } else {
            LOG.info("build agg long sum = {}", sumsLong.get(bucket));
            return new InternalSum(name, sumsLong.get(bucket), format, pipelineAggregators(), metaData());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, sumsLong, compensations);
    }
}
