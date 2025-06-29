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
 * {@link AggregatorFunctionSupplier} implementation for {@link VariancePopulationIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class VariancePopulationIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public VariancePopulationIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return VariancePopulationIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return VariancePopulationIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public VariancePopulationIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return VariancePopulationIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public VariancePopulationIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return VariancePopulationIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "variance_population of ints";
  }
}
