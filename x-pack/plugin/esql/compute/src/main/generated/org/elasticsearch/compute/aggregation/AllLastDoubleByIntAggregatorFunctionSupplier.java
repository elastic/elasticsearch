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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastDoubleByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastDoubleByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastDoubleByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastDoubleByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastDoubleByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastDoubleByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastDoubleByIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllLastDoubleByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllLastDoubleByIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastDoubleByIntAggregator.describe();
  }
}
