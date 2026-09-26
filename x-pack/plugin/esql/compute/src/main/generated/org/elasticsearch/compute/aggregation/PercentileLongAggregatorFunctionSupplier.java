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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double percentile;

  private final double tDigestStateCompression;

  public PercentileLongAggregatorFunctionSupplier(double percentile,
      double tDigestStateCompression) {
    this.percentile = percentile;
    this.tDigestStateCompression = tDigestStateCompression;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new PercentileLongAggregatorFunction(driverContext, channels, percentile, tDigestStateCompression);
  }

  @Override
  public PercentileLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new PercentileLongGroupingAggregatorFunction(channels, driverContext, percentile, tDigestStateCompression);
  }

  @Override
  public String describe() {
    return "percentile of longs";
  }
}
