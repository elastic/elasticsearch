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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctBooleanAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class CountDistinctBooleanAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public CountDistinctBooleanAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return CountDistinctBooleanAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return CountDistinctBooleanGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public CountDistinctBooleanAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return CountDistinctBooleanAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public CountDistinctBooleanGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return CountDistinctBooleanGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "count_distinct of booleans";
  }
}
