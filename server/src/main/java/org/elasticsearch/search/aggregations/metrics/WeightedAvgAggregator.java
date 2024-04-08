/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder.VALUE_FIELD;
import static org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder.WEIGHT_FIELD;

class WeightedAvgAggregator extends NumericMetricsAggregator.SingleValue {

    private final MultiValuesSource.NumericMultiValuesSource valuesSources;

    private DoubleArray weights;
    private DoubleArray valueSums;
    private DoubleArray valueCompensations;
    private DoubleArray weightCompensations;
    private final DocValueFormat format;

    WeightedAvgAggregator(
        String name,
        MultiValuesSource.NumericMultiValuesSource valuesSources,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSources != null;
        this.valuesSources = valuesSources;
        this.format = format;
        weights = bigArrays().newDoubleArray(1, true);
        valueSums = bigArrays().newDoubleArray(1, true);
        valueCompensations = bigArrays().newDoubleArray(1, true);
        weightCompensations = bigArrays().newDoubleArray(1, true);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final SortedNumericDoubleValues docValues = valuesSources.getField(VALUE_FIELD.getPreferredName(), aggCtx.getLeafReaderContext());
        final SortedNumericDoubleValues docWeights = valuesSources.getField(WEIGHT_FIELD.getPreferredName(), aggCtx.getLeafReaderContext());
        final CompensatedSum compensatedValueSum = new CompensatedSum(0, 0);
        final CompensatedSum compensatedWeightSum = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, docValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                weights = bigArrays().grow(weights, bucket + 1);
                valueSums = bigArrays().grow(valueSums, bucket + 1);
                valueCompensations = bigArrays().grow(valueCompensations, bucket + 1);
                weightCompensations = bigArrays().grow(weightCompensations, bucket + 1);

                if (docValues.advanceExact(doc) && docWeights.advanceExact(doc)) {
                    if (docWeights.docValueCount() > 1) {
                        throw new IllegalArgumentException(
                            "Encountered more than one weight for a "
                                + "single document. Use a script to combine multiple weights-per-doc into a single value."
                        );
                    }
                    // There should always be one weight if advanceExact lands us here, either
                    // a real weight or a `missing` weight
                    assert docWeights.docValueCount() == 1;
                    final double weight = docWeights.nextValue();

                    final int numValues = docValues.docValueCount();
                    assert numValues > 0;

                    double valueSum = valueSums.get(bucket);
                    double valueCompensation = valueCompensations.get(bucket);
                    compensatedValueSum.reset(valueSum, valueCompensation);

                    double weightSum = weights.get(bucket);
                    double weightCompensation = weightCompensations.get(bucket);
                    compensatedWeightSum.reset(weightSum, weightCompensation);

                    for (int i = 0; i < numValues; i++) {
                        compensatedValueSum.add(docValues.nextValue() * weight);
                        compensatedWeightSum.add(weight);
                    }

                    valueSums.set(bucket, compensatedValueSum.value());
                    valueCompensations.set(bucket, compensatedValueSum.delta());
                    weights.set(bucket, compensatedWeightSum.value());
                    weightCompensations.set(bucket, compensatedWeightSum.delta());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= valueSums.size()) {
            return Double.NaN;
        }
        return valueSums.get(owningBucketOrd) / weights.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= valueSums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalWeightedAvg(name, valueSums.get(bucket), weights.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalWeightedAvg.empty(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(weights, valueSums, valueCompensations, weightCompensations);
    }

}
