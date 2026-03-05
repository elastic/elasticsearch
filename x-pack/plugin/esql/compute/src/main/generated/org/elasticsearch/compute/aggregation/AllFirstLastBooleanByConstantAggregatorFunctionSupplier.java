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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstLastBooleanByConstantAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstLastBooleanByConstantAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstLastBooleanByConstantAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstLastBooleanByConstantAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstLastBooleanByConstantGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstLastBooleanByConstantAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstLastBooleanByConstantAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstLastBooleanByConstantGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstLastBooleanByConstantGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstLastBooleanByConstantAggregator.describe();
  }
}
