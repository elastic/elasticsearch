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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstFloatByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstFloatByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstFloatByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstFloatByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstFloatByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstFloatByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstFloatByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstFloatByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstFloatByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstFloatByTimestampAggregator.describe();
  }
}
