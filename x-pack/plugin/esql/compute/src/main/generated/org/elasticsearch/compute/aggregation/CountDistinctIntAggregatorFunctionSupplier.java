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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class CountDistinctIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int precision;

  public CountDistinctIntAggregatorFunctionSupplier(int precision) {
    this.precision = precision;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return CountDistinctIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return CountDistinctIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public CountDistinctIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return CountDistinctIntAggregatorFunction.create(driverContext, channels, precision);
  }

  @Override
  public CountDistinctIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return CountDistinctIntGroupingAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of ints";
  }
}
