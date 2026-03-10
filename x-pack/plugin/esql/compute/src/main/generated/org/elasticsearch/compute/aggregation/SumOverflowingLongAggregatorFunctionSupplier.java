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
 * {@link AggregatorFunctionSupplier} implementation for {@link SumOverflowingLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SumOverflowingLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SumOverflowingLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SumOverflowingLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SumOverflowingLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SumOverflowingLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SumOverflowingLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SumOverflowingLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SumOverflowingLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "sum_overflowing of longs";
  }
}
