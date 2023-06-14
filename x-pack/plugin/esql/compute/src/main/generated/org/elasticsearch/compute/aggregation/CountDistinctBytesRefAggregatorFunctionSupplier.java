// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.util.BigArrays;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctBytesRefAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final int channel;

  private final int precision;

  public CountDistinctBytesRefAggregatorFunctionSupplier(BigArrays bigArrays, int channel,
      int precision) {
    this.bigArrays = bigArrays;
    this.channel = channel;
    this.precision = precision;
  }

  @Override
  public CountDistinctBytesRefAggregatorFunction aggregator() {
    return CountDistinctBytesRefAggregatorFunction.create(channel, bigArrays, precision);
  }

  @Override
  public CountDistinctBytesRefGroupingAggregatorFunction groupingAggregator() {
    return CountDistinctBytesRefGroupingAggregatorFunction.create(channel, bigArrays, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of bytes";
  }
}
