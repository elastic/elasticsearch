// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.util.BigArrays;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class PercentileLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final int channel;

  private final double percentile;

  public PercentileLongAggregatorFunctionSupplier(BigArrays bigArrays, int channel,
      double percentile) {
    this.bigArrays = bigArrays;
    this.channel = channel;
    this.percentile = percentile;
  }

  @Override
  public PercentileLongAggregatorFunction aggregator() {
    return PercentileLongAggregatorFunction.create(channel, percentile);
  }

  @Override
  public PercentileLongGroupingAggregatorFunction groupingAggregator() {
    return PercentileLongGroupingAggregatorFunction.create(channel, bigArrays, percentile);
  }

  @Override
  public String describe() {
    return "percentile of longs";
  }
}
