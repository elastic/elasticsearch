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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstLastFloatByConstantAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstLastFloatByConstantAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstLastFloatByConstantAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstLastFloatByConstantAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstLastFloatByConstantGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstLastFloatByConstantAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return AllFirstLastFloatByConstantAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public AllFirstLastFloatByConstantGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return AllFirstLastFloatByConstantGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstLastFloatByConstantAggregator.describe();
  }
}
