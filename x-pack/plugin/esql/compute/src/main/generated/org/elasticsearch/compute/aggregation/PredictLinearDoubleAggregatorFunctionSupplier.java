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
 * {@link AggregatorFunctionSupplier} implementation for {@link PredictLinearDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PredictLinearDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double horizon;

  public PredictLinearDoubleAggregatorFunctionSupplier(double horizon) {
    this.horizon = horizon;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PredictLinearDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public PredictLinearDoubleGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new PredictLinearDoubleGroupingAggregatorFunction(channels, driverContext, horizon);
  }

  @Override
  public String describe() {
    return "predict_linear of doubles";
  }
}
