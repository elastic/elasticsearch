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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class CountDistinctFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int precision;

  public CountDistinctFloatAggregatorFunctionSupplier(int precision) {
    this.precision = precision;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return CountDistinctFloatAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return CountDistinctFloatGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public CountDistinctFloatAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return CountDistinctFloatAggregatorFunction.create(driverContext, channels, precision);
  }

  @Override
  public CountDistinctFloatGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return CountDistinctFloatGroupingAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of floats";
  }
}
