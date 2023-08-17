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

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  private final int precision;

  public CountDistinctDoubleAggregatorFunctionSupplier(BigArrays bigArrays, List<Integer> channels,
      int precision) {
    this.bigArrays = bigArrays;
    this.channels = channels;
    this.precision = precision;
  }

  @Override
  public CountDistinctDoubleAggregatorFunction aggregator() {
    return CountDistinctDoubleAggregatorFunction.create(channels, bigArrays, precision);
  }

  @Override
  public CountDistinctDoubleGroupingAggregatorFunction groupingAggregator() {
    return CountDistinctDoubleGroupingAggregatorFunction.create(channels, bigArrays, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of doubles";
  }
}
