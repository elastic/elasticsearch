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
 * {@link AggregatorFunctionSupplier} implementation for {@link SumIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SumIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SumIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SumIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SumIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SumIntAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    return SumIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SumIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SumIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "sum of ints";
  }
}
