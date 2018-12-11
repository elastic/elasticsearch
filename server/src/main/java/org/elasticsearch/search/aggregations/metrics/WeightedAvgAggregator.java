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
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder.VALUE_FIELD;
import static org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder.WEIGHT_FIELD;

class WeightedAvgAggregator extends NumericMetricsAggregator.SingleValue {

    private final MultiValuesSource.NumericMultiValuesSource valuesSources;

    private DoubleArray weights;
    private DoubleArray sums;
    private DoubleArray sumCompensations;
    private DoubleArray weightCompensations;
    private DocValueFormat format;

    WeightedAvgAggregator(String name, MultiValuesSource.NumericMultiValuesSource valuesSources, DocValueFormat format,
                            SearchContext context, Aggregator parent,
                            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSources = valuesSources;
        this.format = format;
        if (valuesSources != null) {
            final BigArrays bigArrays = context.bigArrays();
            weights = bigArrays.newDoubleArray(1, true);
            sums = bigArrays.newDoubleArray(1, true);
            sumCompensations = bigArrays.newDoubleArray(1, true);
            weightCompensations = bigArrays.newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSources != null && valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues docValues = valuesSources.getField(VALUE_FIELD.getPreferredName(), ctx);
        final SortedNumericDoubleValues docWeights = valuesSources.getField(WEIGHT_FIELD.getPreferredName(), ctx);

        return new LeafBucketCollectorBase(sub, docValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                weights = bigArrays.grow(weights, bucket + 1);
                sums = bigArrays.grow(sums, bucket + 1);
                sumCompensations = bigArrays.grow(sumCompensations, bucket + 1);
                weightCompensations = bigArrays.grow(weightCompensations, bucket + 1);

                if (docValues.advanceExact(doc) && docWeights.advanceExact(doc)) {
                    if (docWeights.docValueCount() > 1) {
                        throw new AggregationExecutionException("Encountered more than one weight for a " +
                            "single document. Use a script to combine multiple weights-per-doc into a single value.");
                    }
                    // There should always be one weight if advanceExact lands us here, either
                    // a real weight or a `missing` weight
                    assert docWeights.docValueCount() == 1;
                    final double weight = docWeights.nextValue();

                    final int numValues = docValues.docValueCount();
                    assert numValues > 0;

                    for (int i = 0; i < numValues; i++) {
                        kahanSum(docValues.nextValue() * weight, sums, sumCompensations, bucket);
                        kahanSum(weight, weights, weightCompensations, bucket);
                    }
                }
            }
        };
    }

    private static void kahanSum(double value, DoubleArray values, DoubleArray compensations, long bucket) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        double sum = values.get(bucket);
        double compensation = compensations.get(bucket);

        if (Double.isFinite(value) == false) {
            sum += value;
        } else if (Double.isFinite(sum)) {
            double corrected = value - compensation;
            double newSum = sum + corrected;
            compensation = (newSum - sum) - corrected;
            sum = newSum;
        }
        values.set(bucket, sum);
        compensations.set(bucket, compensation);
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSources == null || owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / weights.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalWeightedAvg(name, sums.get(bucket), weights.get(bucket), format, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalWeightedAvg(name, 0.0, 0L, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(weights, sums, sumCompensations, weightCompensations);
    }

}
