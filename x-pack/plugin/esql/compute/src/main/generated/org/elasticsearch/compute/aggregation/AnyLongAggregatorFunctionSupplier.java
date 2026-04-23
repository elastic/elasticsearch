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
 * {@link AggregatorFunctionSupplier} implementation for {@link AnyLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AnyLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AnyLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AnyLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AnyLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AnyLongAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    return new AnyLongAggregatorFunction(driverContext, channels);
  }

  @Override
  public AnyLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyLongGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AnyLongAggregator.describe();
  }
}
