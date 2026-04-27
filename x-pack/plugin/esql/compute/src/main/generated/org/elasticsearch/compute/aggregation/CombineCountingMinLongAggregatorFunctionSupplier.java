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
 * {@link AggregatorFunctionSupplier} implementation for {@link CombineCountingMinLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class CombineCountingMinLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public CombineCountingMinLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return CombineCountingMinLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return CombineCountingMinLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public CombineCountingMinLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new CombineCountingMinLongAggregatorFunction(driverContext, channels);
  }

  @Override
  public CombineCountingMinLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new CombineCountingMinLongGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return "combine_counting_min of longs";
  }
}
