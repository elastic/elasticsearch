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
 * {@link AggregatorFunctionSupplier} implementation for {@link MinNonFiniteDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MinNonFiniteDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MinNonFiniteDoubleAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MinNonFiniteDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MinNonFiniteDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MinNonFiniteDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new MinNonFiniteDoubleAggregatorFunction(driverContext, channels);
  }

  @Override
  public MinNonFiniteDoubleGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new MinNonFiniteDoubleGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min_non_finite of doubles";
  }
}
