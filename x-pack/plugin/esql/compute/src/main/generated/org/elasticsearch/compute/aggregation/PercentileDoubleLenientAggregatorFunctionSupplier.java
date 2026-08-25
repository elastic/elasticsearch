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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileDoubleLenientAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileDoubleLenientAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double percentile;

  private final double tDigestStateCompression;

  public PercentileDoubleLenientAggregatorFunctionSupplier(double percentile,
      double tDigestStateCompression) {
    this.percentile = percentile;
    this.tDigestStateCompression = tDigestStateCompression;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileDoubleLenientAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileDoubleLenientGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileDoubleLenientAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new PercentileDoubleLenientAggregatorFunction(driverContext, channels, percentile, tDigestStateCompression);
  }

  @Override
  public PercentileDoubleLenientGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new PercentileDoubleLenientGroupingAggregatorFunction(channels, driverContext, percentile, tDigestStateCompression);
  }

  @Override
  public String describe() {
    return "percentile_double of lenients";
  }
}
