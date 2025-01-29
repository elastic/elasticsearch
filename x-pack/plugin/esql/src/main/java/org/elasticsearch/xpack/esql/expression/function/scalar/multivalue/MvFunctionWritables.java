/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

public class MvFunctionWritables {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            MvAppend.ENTRY,
            MvAvg.ENTRY,
            MvConcat.ENTRY,
            MvCount.ENTRY,
            MvDedupe.ENTRY,
            MvFirst.ENTRY,
            MvLast.ENTRY,
            MvMax.ENTRY,
            MvMedian.ENTRY,
            MvMedianAbsoluteDeviation.ENTRY,
            MvMin.ENTRY,
            MvPercentile.ENTRY,
            MvPSeriesWeightedSum.ENTRY,
            MvSlice.ENTRY,
            MvSort.ENTRY,
            MvSum.ENTRY,
            MvZip.ENTRY
        );
    }
}
