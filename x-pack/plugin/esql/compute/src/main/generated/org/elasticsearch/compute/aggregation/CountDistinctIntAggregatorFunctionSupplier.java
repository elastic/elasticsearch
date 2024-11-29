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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  private final int precision;

  public CountDistinctIntAggregatorFunctionSupplier(List<Integer> channels, int precision) {
    this.channels = channels;
    this.precision = precision;
  }

  @Override
  public CountDistinctIntAggregatorFunction aggregator(DriverContext driverContext) {
    return CountDistinctIntAggregatorFunction.create(driverContext, channels, precision);
  }

  @Override
  public CountDistinctIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return CountDistinctIntGroupingAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of ints";
  }
}
