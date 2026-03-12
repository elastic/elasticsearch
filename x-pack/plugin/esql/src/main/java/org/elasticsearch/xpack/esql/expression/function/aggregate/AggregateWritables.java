/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

public class AggregateWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            Avg.ENTRY,
            Count.ENTRY,
            CountDistinct.ENTRY,
            First.ENTRY,
            Max.ENTRY,
            Median.ENTRY,
            MedianAbsoluteDeviation.ENTRY,
            Min.ENTRY,
            Percentile.ENTRY,
            Rate.ENTRY,
            Irate.ENTRY,
            Idelta.ENTRY,
            Increase.ENTRY,
            Delta.ENTRY,
            Deriv.ENTRY,
            Sample.ENTRY,
            Scalar.ENTRY,
            SpatialCentroid.ENTRY,
            SpatialExtent.ENTRY,
            StdDev.ENTRY,
            Variance.ENTRY,
            Sum.ENTRY,
            Top.ENTRY,
            Values.ENTRY,
            MinOverTime.ENTRY,
            MaxOverTime.ENTRY,
            AvgOverTime.ENTRY,
            Last.ENTRY,
            LastOverTime.ENTRY,
            FirstOverTime.ENTRY,
            SumOverTime.ENTRY,
            CountOverTime.ENTRY,
            CountDistinctOverTime.ENTRY,
            WeightedAvg.ENTRY,
            Present.ENTRY,
            PresentOverTime.ENTRY,
            Absent.ENTRY,
            AbsentOverTime.ENTRY,
            DimensionValues.ENTRY,
            HistogramMerge.ENTRY,
            HistogramMergeOverTime.ENTRY
        );
    }
}
