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
            Max.ENTRY,
            Median.ENTRY,
            MedianAbsoluteDeviation.ENTRY,
            Min.ENTRY,
            Percentile.ENTRY,
            Rate.ENTRY,
            Sample.ENTRY,
            SpatialCentroid.ENTRY,
            SpatialExtent.ENTRY,
            StdDev.ENTRY,
            Sum.ENTRY,
            Top.ENTRY,
            Values.ENTRY,
            MinOverTime.ENTRY,
            MaxOverTime.ENTRY,
            AvgOverTime.ENTRY,
            LastOverTime.ENTRY,
            FirstOverTime.ENTRY,
            // internal functions
            ToPartial.ENTRY,
            FromPartial.ENTRY,
            WeightedAvg.ENTRY
        );
    }
}
