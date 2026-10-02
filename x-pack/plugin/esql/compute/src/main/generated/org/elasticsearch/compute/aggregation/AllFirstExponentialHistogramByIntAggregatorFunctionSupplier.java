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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstExponentialHistogramByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstExponentialHistogramByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstExponentialHistogramByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstExponentialHistogramByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstExponentialHistogramByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstExponentialHistogramByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AllFirstExponentialHistogramByIntAggregatorFunction(driverContext, channels);
  }

  @Override
  public AllFirstExponentialHistogramByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new AllFirstExponentialHistogramByIntGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstExponentialHistogramByIntAggregator.describe();
  }
}
