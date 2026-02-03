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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstBooleanByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstBooleanByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstBooleanByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstBooleanByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstBooleanByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstBooleanByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstBooleanByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstBooleanByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstBooleanByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstBooleanByTimestampAggregator.describe();
  }
}
