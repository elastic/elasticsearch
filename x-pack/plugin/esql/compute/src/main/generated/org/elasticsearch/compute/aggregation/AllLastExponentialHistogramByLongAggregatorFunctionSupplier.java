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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastExponentialHistogramByLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastExponentialHistogramByLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastExponentialHistogramByLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastExponentialHistogramByLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastExponentialHistogramByLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastExponentialHistogramByLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AllLastExponentialHistogramByLongAggregatorFunction(driverContext, channels);
  }

  @Override
  public AllLastExponentialHistogramByLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new AllLastExponentialHistogramByLongGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastExponentialHistogramByLongAggregator.describe();
  }
}
