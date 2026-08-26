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
 * {@link AggregatorFunctionSupplier} implementation for {@link AnyFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AnyFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AnyFloatAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AnyFloatAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AnyFloatGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AnyFloatAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyFloatAggregatorFunction(driverContext, channels);
  }

  @Override
  public AnyFloatGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyFloatGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AnyFloatAggregator.describe();
  }
}
