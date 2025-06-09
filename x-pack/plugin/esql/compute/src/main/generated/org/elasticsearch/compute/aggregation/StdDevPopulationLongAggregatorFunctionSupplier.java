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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDevPopulationLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class StdDevPopulationLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public StdDevPopulationLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return StdDevPopulationLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return StdDevPopulationLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public StdDevPopulationLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevPopulationLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDevPopulationLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return StdDevPopulationLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_dev_population of longs";
  }
}
