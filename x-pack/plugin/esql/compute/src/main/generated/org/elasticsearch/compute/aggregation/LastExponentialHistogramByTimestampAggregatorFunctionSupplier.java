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
 * {@link AggregatorFunctionSupplier} implementation for {@link LastExponentialHistogramByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class LastExponentialHistogramByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public LastExponentialHistogramByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return LastExponentialHistogramByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return LastExponentialHistogramByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public LastExponentialHistogramByTimestampAggregatorFunction aggregator(
      DriverContext driverContext, List<Integer> channels) {
    return LastExponentialHistogramByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public LastExponentialHistogramByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return LastExponentialHistogramByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return LastExponentialHistogramByTimestampAggregator.describe();
  }
}
