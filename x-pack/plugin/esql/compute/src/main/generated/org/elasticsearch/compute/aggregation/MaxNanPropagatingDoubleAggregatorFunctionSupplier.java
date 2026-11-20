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
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxNanPropagatingDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MaxNanPropagatingDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MaxNanPropagatingDoubleAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MaxNanPropagatingDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MaxNanPropagatingDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MaxNanPropagatingDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new MaxNanPropagatingDoubleAggregatorFunction(driverContext, channels);
  }

  @Override
  public MaxNanPropagatingDoubleGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new MaxNanPropagatingDoubleGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return "max_nan_propagating of doubles";
  }
}
