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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstFloatByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstFloatByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstFloatByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstFloatByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstFloatByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstFloatByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstFloatByIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstFloatByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstFloatByIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstFloatByIntAggregator.describe();
  }
}
