/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

public final class AggregatorFunctionProviders {

    private AggregatorFunctionProviders() {}

    public static AggregatorFunction.Provider avgDouble() {
        return DoubleAvgAggregator::create;
    }

    public static AggregatorFunction.Provider avgLong() {
        return LongAvgAggregator::create;
    }

    public static AggregatorFunction.Provider count() {
        return CountRowsAggregator::create;
    }

    public static AggregatorFunction.Provider max() {
        return MaxAggregator::create;
    }

    public static AggregatorFunction.Provider min() {
        return MinAggregator::create;
    }

    public static AggregatorFunction.Provider sum() {
        return SumAggregator::create;
    }
}
