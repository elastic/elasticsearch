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
 * {@link AggregatorFunctionSupplier} implementation for {@link DerivIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class DerivIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public DerivIntAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return DerivIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return DerivIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public DerivIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return DerivIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public DerivIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return DerivIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "deriv of ints";
  }
}
