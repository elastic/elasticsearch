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
 * {@link AggregatorFunctionSupplier} implementation for {@link VarianceSampleIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class VarianceSampleIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public VarianceSampleIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return VarianceSampleIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return VarianceSampleIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public VarianceSampleIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return VarianceSampleIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public VarianceSampleIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return VarianceSampleIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "variance_sample of ints";
  }
}
