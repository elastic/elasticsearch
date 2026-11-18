/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.exponentialhistogram;

import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.sourcebatch.SourceSchema;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_COUNTS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_INDICES_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MAX_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MIN_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.NEGATIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.POSITIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SCALE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SUM_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_COUNT_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD;

/**
 * The columns of an {@code exponential_histogram} field value, as {@link ExponentialHistogramXContent} writes it, within a
 * {@link SourceSchema}. Writers that build source batches directly, rather than through XContent, use it to share one
 * definition of the layout the {@code exponential_histogram} mapper parses.
 *
 * <p>The bucket indices and counts of each range are parallel arrays.
 */
public record ExponentialHistogramEscfSchema(
    int scaleColIdx,
    int zeroCountColIdx,
    int zeroThresholdColIdx,
    int negativeIndicesColIdx,
    int negativeCountsColIdx,
    int positiveIndicesColIdx,
    int positiveCountsColIdx,
    int sumColIdx,
    int minColIdx,
    int maxColIdx
) {

    /** Registers the fields of an {@code exponential_histogram} value, see {@link SourceSchema#getSubSchema}. */
    public static final SourceSchema.SubSchemaFactory<ExponentialHistogramEscfSchema> FACTORY = (schema, parentIdx, name) -> {
        int histogram = schema.appendNonLeaf(name, parentIdx);
        int scale = schema.appendLeaf(SCALE_FIELD, histogram);
        int zero = schema.appendNonLeaf(ZERO_FIELD, histogram);
        int zeroCount = schema.appendLeaf(ZERO_COUNT_FIELD, zero);
        int zeroThreshold = schema.appendLeaf(ZERO_THRESHOLD_FIELD, zero);
        int negative = schema.appendNonLeaf(NEGATIVE_FIELD, histogram);
        int negativeIndices = schema.appendLeaf(BUCKET_INDICES_FIELD, negative);
        int negativeCounts = schema.appendLeaf(BUCKET_COUNTS_FIELD, negative);
        int positive = schema.appendNonLeaf(POSITIVE_FIELD, histogram);
        int positiveIndices = schema.appendLeaf(BUCKET_INDICES_FIELD, positive);
        int positiveCounts = schema.appendLeaf(BUCKET_COUNTS_FIELD, positive);
        int sum = schema.appendLeaf(SUM_FIELD, histogram);
        int min = schema.appendLeaf(MIN_FIELD, histogram);
        int max = schema.appendLeaf(MAX_FIELD, histogram);
        return new ExponentialHistogramEscfSchema(
            scale,
            zeroCount,
            zeroThreshold,
            negativeIndices,
            negativeCounts,
            positiveIndices,
            positiveCounts,
            sum,
            min,
            max
        );
    };
}
