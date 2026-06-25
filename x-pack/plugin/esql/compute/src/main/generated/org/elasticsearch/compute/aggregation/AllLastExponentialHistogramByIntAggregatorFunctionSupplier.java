// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastExponentialHistogramByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastExponentialHistogramByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastExponentialHistogramByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastExponentialHistogramByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastExponentialHistogramByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastExponentialHistogramByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AllLastExponentialHistogramByIntAggregatorFunction(driverContext, channels);
  }

  @Override
  public AllLastExponentialHistogramByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new AllLastExponentialHistogramByIntGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastExponentialHistogramByIntAggregator.describe();
  }
}
