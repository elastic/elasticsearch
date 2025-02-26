/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.histogram.SizedBucketAggregator;

/**
 * A {@link SingleDimensionValuesSource} for date histogram values.
 */
public class DateHistogramValuesSource extends LongValuesSource implements SizedBucketAggregator {
    private final RoundingValuesSource preparedRounding;

    DateHistogramValuesSource(
        BigArrays bigArrays,
        MappedFieldType fieldType,
        RoundingValuesSource roundingValuesSource,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(
            bigArrays,
            fieldType,
            roundingValuesSource::longValues,
            roundingValuesSource::round,
            format,
            missingBucket,
            missingOrder,
            size,
            reverseMul
        );
        this.preparedRounding = roundingValuesSource;
    }

    @Override
    public double bucketSize(long bucket, Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            Long value = toComparable((int) bucket);
            assert value != null : "unexpected null value in composite agg bucket [" + (int) bucket + "]";
            return preparedRounding.roundingSize(value, unitSize);
        } else {
            return 1.0;
        }
    }

    @Override
    public double bucketSize(Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            return preparedRounding.roundingSize(unitSize);
        } else {
            return 1.0;
        }
    }

}
