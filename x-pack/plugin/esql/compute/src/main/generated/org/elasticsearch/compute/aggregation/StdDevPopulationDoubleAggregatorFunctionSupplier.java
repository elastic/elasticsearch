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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDevPopulationDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class StdDevPopulationDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public StdDevPopulationDoubleAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return StdDevPopulationDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return StdDevPopulationDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public StdDevPopulationDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevPopulationDoubleAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDevPopulationDoubleGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return StdDevPopulationDoubleGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_dev_population of doubles";
  }
}
