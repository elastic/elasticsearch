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
 * {@link AggregatorFunctionSupplier} implementation for {@link AnyBooleanAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AnyBooleanAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AnyBooleanAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AnyBooleanAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AnyBooleanGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AnyBooleanAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AnyBooleanAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AnyBooleanGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AnyBooleanGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AnyBooleanAggregator.describe();
  }
}
