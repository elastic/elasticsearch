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
 * {@link AggregatorFunctionSupplier} implementation for {@link MinFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MinFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MinFloatAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MinFloatAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MinFloatGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MinFloatAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MinFloatAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MinFloatGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MinFloatGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min of floats";
  }
}
