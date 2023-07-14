// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class PercentileDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  private final double percentile;

  public PercentileDoubleAggregatorFunctionSupplier(BigArrays bigArrays, List<Integer> channels,
      double percentile) {
    this.bigArrays = bigArrays;
    this.channels = channels;
    this.percentile = percentile;
  }

  @Override
  public PercentileDoubleAggregatorFunction aggregator() {
    return PercentileDoubleAggregatorFunction.create(channels, percentile);
  }

  @Override
  public PercentileDoubleGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return PercentileDoubleGroupingAggregatorFunction.create(channels, driverContext, bigArrays, percentile);
  }

  @Override
  public String describe() {
    return "percentile of doubles";
  }
}
