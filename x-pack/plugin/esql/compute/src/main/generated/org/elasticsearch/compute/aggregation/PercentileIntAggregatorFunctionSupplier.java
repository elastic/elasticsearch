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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double percentile;

  public PercentileIntAggregatorFunctionSupplier(double percentile) {
    this.percentile = percentile;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileIntAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return PercentileIntAggregatorFunction.create(driverContext, channels, percentile);
  }

  @Override
  public PercentileIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return PercentileIntGroupingAggregatorFunction.create(channels, driverContext, percentile);
  }

  @Override
  public String describe() {
    return "percentile of ints";
  }
}
