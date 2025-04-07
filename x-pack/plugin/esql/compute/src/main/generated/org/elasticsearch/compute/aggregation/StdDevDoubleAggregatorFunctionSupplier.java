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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDevDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class StdDevDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public StdDevDoubleAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return StdDevDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return StdDevDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public StdDevDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevDoubleAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDevDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevDoubleGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_dev of doubles";
  }
}
