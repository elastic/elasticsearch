// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.util.BigArrays;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link MinLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final int channel;

  public MinLongAggregatorFunctionSupplier(BigArrays bigArrays, int channel) {
    this.bigArrays = bigArrays;
    this.channel = channel;
  }

  @Override
  public MinLongAggregatorFunction aggregator() {
    return MinLongAggregatorFunction.create(channel);
  }

  @Override
  public MinLongGroupingAggregatorFunction groupingAggregator() {
    return MinLongGroupingAggregatorFunction.create(channel, bigArrays);
  }

  @Override
  public String describe() {
    return "min of longs";
  }
}
