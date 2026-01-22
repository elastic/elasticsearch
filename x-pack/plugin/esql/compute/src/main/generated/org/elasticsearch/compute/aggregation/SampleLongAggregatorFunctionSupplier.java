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
 * {@link AggregatorFunctionSupplier} implementation for {@link SampleLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SampleLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  public SampleLongAggregatorFunctionSupplier(int limit) {
    this.limit = limit;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SampleLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SampleLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SampleLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SampleLongAggregatorFunction.create(driverContext, channels, limit);
  }

  @Override
  public SampleLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SampleLongGroupingAggregatorFunction.create(channels, driverContext, limit);
  }

  @Override
  public String describe() {
    return "sample of longs";
  }
}
