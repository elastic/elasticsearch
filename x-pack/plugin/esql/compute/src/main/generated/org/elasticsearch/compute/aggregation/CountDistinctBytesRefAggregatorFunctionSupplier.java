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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctBytesRefAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  private final int precision;

  public CountDistinctBytesRefAggregatorFunctionSupplier(BigArrays bigArrays,
      List<Integer> channels, int precision) {
    this.bigArrays = bigArrays;
    this.channels = channels;
    this.precision = precision;
  }

  @Override
  public CountDistinctBytesRefAggregatorFunction aggregator() {
    return CountDistinctBytesRefAggregatorFunction.create(channels, bigArrays, precision);
  }

  @Override
  public CountDistinctBytesRefGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return CountDistinctBytesRefGroupingAggregatorFunction.create(channels, driverContext, bigArrays, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of bytes";
  }
}
