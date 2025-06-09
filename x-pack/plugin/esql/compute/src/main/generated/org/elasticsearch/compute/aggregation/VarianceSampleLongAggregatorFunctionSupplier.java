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
 * {@link AggregatorFunctionSupplier} implementation for {@link VarianceSampleLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class VarianceSampleLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public VarianceSampleLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return VarianceSampleLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return VarianceSampleLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public VarianceSampleLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return VarianceSampleLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public VarianceSampleLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return VarianceSampleLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "variance_sample of longs";
  }
}
