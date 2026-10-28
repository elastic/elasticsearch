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
 * {@link AggregatorFunctionSupplier} implementation for {@link AllFirstTDigestByLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AllFirstTDigestByLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AllFirstTDigestByLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AllFirstTDigestByLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AllFirstTDigestByLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AllFirstTDigestByLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AllFirstTDigestByLongAggregatorFunction(driverContext, channels);
  }

  @Override
  public AllFirstTDigestByLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new AllFirstTDigestByLongGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AllFirstTDigestByLongAggregator.describe();
  }
}
