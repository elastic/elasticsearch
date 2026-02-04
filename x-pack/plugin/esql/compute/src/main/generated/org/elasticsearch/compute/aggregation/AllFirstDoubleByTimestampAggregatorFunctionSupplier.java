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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstDoubleByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstDoubleByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstDoubleByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstDoubleByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstDoubleByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstDoubleByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstDoubleByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstDoubleByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstDoubleByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstDoubleByTimestampAggregator.describe();
  }
}
