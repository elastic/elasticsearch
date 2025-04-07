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
 * {@link AggregatorFunctionSupplier} implementation for {@link MinDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MinDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MinDoubleAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MinDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MinDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MinDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MinDoubleAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MinDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MinDoubleGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min of doubles";
  }
}
