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
 * {@link AggregatorFunctionSupplier} implementation for {@link SampleIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SampleIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  public SampleIntAggregatorFunctionSupplier(int limit) {
    this.limit = limit;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SampleIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SampleIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SampleIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SampleIntAggregatorFunction.create(driverContext, channels, limit);
  }

  @Override
  public SampleIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SampleIntGroupingAggregatorFunction.create(channels, driverContext, limit);
  }

  @Override
  public String describe() {
    return "sample of ints";
  }
}
