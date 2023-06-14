// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.util.BigArrays;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final int channel;

  public SumLongAggregatorFunctionSupplier(BigArrays bigArrays, int channel) {
    this.bigArrays = bigArrays;
    this.channel = channel;
  }

  @Override
  public SumLongAggregatorFunction aggregator() {
    return SumLongAggregatorFunction.create(channel);
  }

  @Override
  public SumLongGroupingAggregatorFunction groupingAggregator() {
    return SumLongGroupingAggregatorFunction.create(channel, bigArrays);
  }

  @Override
  public String describe() {
    return "sum of longs";
  }
}
