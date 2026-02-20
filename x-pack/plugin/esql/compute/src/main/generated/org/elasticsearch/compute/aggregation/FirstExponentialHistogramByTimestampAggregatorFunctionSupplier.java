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
 * {@link AggregatorFunctionSupplier} implementation for {@link FirstExponentialHistogramByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class FirstExponentialHistogramByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public FirstExponentialHistogramByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return FirstExponentialHistogramByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return FirstExponentialHistogramByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public FirstExponentialHistogramByTimestampAggregatorFunction aggregator(
      DriverContext driverContext, List<Integer> channels) {
    return FirstExponentialHistogramByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public FirstExponentialHistogramByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return FirstExponentialHistogramByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return FirstExponentialHistogramByTimestampAggregator.describe();
  }
}
