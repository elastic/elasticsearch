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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstBooleanByIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstBooleanByIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstBooleanByIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstBooleanByIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstBooleanByIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstBooleanByIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstBooleanByIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstBooleanByIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstBooleanByIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstBooleanByIntAggregator.describe();
  }
}
