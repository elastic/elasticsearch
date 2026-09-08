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
 * {@link AggregatorFunctionSupplier} implementation for {@link MinDoubleLenientAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MinDoubleLenientAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MinDoubleLenientAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MinDoubleLenientAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MinDoubleLenientGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MinDoubleLenientAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new MinDoubleLenientAggregatorFunction(driverContext, channels);
  }

  @Override
  public MinDoubleLenientGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new MinDoubleLenientGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min_double of lenients";
  }
}
