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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastBooleanByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastBooleanByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastBooleanByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastBooleanByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastBooleanByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastBooleanByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastBooleanByIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllLastBooleanByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllLastBooleanByIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastBooleanByIntAggregator.describe();
  }
}
