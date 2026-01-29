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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllLastFloatByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllLastFloatByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllLastFloatByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllLastFloatByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllLastFloatByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllLastFloatByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllLastFloatByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllLastFloatByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllLastFloatByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllLastFloatByTimestampAggregator.describe();
  }
}
