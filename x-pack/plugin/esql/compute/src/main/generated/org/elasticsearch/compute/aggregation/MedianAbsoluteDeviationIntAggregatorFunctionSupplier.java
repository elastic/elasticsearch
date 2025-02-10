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
 * {@link AggregatorFunctionSupplier} implementation for {@link MedianAbsoluteDeviationIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MedianAbsoluteDeviationIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MedianAbsoluteDeviationIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MedianAbsoluteDeviationIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MedianAbsoluteDeviationIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MedianAbsoluteDeviationIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MedianAbsoluteDeviationIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MedianAbsoluteDeviationIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return MedianAbsoluteDeviationIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "median_absolute_deviation of ints";
  }
}
