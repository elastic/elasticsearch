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
 * {@link AggregatorFunctionSupplier} implementation for {@link VariancePopulationLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class VariancePopulationLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public VariancePopulationLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return VariancePopulationLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return VariancePopulationLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public VariancePopulationLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return VariancePopulationLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public VariancePopulationLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return VariancePopulationLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "variance_population of longs";
  }
}
