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
 * {@link AggregatorFunctionSupplier} implementation for {@link AnyExponentialHistogramAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AnyExponentialHistogramAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AnyExponentialHistogramAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AnyExponentialHistogramAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AnyExponentialHistogramGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AnyExponentialHistogramAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyExponentialHistogramAggregatorFunction(driverContext, channels);
  }

  @Override
  public AnyExponentialHistogramGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new AnyExponentialHistogramGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AnyExponentialHistogramAggregator.describe();
  }
}
