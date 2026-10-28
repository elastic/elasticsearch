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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final double percentile;

  private final double tDigestStateCompression;

  private final boolean allowNonFinite;

  public PercentileDoubleAggregatorFunctionSupplier(double percentile,
      double tDigestStateCompression, boolean allowNonFinite) {
    this.percentile = percentile;
    this.tDigestStateCompression = tDigestStateCompression;
    this.allowNonFinite = allowNonFinite;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new PercentileDoubleAggregatorFunction(driverContext, channels, percentile, tDigestStateCompression, allowNonFinite);
  }

  @Override
  public PercentileDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new PercentileDoubleGroupingAggregatorFunction(channels, driverContext, percentile, tDigestStateCompression, allowNonFinite);
  }

  @Override
  public String describe() {
    return "percentile of doubles";
  }
}
