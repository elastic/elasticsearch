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
 * {@link AggregatorFunctionSupplier} implementation for {@link SparklineLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SparklineLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  private final boolean ascending;

  public SparklineLongAggregatorFunctionSupplier(int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SparklineLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SparklineLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SparklineLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SparklineLongAggregatorFunction.create(driverContext, channels, limit, ascending);
  }

  @Override
  public SparklineLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SparklineLongGroupingAggregatorFunction.create(channels, driverContext, limit, ascending);
  }

  @Override
  public String describe() {
    return "sparkline of longs";
  }
}
