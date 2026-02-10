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
 * {@link AggregatorFunctionSupplier} implementation for {@link SumDenseVectorAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SumDenseVectorAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SumDenseVectorAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SumDenseVectorAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SumDenseVectorGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SumDenseVectorAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SumDenseVectorAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SumDenseVectorGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SumDenseVectorGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return SumDenseVectorAggregator.describe();
  }
}
