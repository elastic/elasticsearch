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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastDoubleByLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastDoubleByLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastDoubleByLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastDoubleByLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastDoubleByLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastDoubleByLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastDoubleByLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllLastDoubleByLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllLastDoubleByLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastDoubleByLongAggregator.describe();
  }
}
