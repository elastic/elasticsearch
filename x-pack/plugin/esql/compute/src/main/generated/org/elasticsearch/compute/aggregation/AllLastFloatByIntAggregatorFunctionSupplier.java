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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastFloatByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastFloatByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastFloatByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastFloatByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastFloatByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastFloatByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastFloatByIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllLastFloatByIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastFloatByIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastFloatByIntAggregator.describe();
  }
}
