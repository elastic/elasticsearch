// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.util.BigArrays;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link AvgLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class AvgLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final int channel;

  public AvgLongAggregatorFunctionSupplier(BigArrays bigArrays, int channel) {
    this.bigArrays = bigArrays;
    this.channel = channel;
  }

  @Override
  public AvgLongAggregatorFunction aggregator() {
    return AvgLongAggregatorFunction.create(channel);
  }

  @Override
  public AvgLongGroupingAggregatorFunction groupingAggregator() {
    return AvgLongGroupingAggregatorFunction.create(channel, bigArrays);
  }

  @Override
  public String describe() {
    return "avg of longs";
  }
}
