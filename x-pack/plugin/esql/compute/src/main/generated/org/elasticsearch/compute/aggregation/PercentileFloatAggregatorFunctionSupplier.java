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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double percentile;

  public PercentileFloatAggregatorFunctionSupplier(double percentile) {
    this.percentile = percentile;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileFloatAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileFloatGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileFloatAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return PercentileFloatAggregatorFunction.create(driverContext, channels, percentile);
  }

  @Override
  public PercentileFloatGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return PercentileFloatGroupingAggregatorFunction.create(channels, driverContext, percentile);
  }

  @Override
  public String describe() {
    return "percentile of floats";
  }
}
