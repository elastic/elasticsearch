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
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class CountDistinctBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  private final int precision;

  public CountDistinctBytesRefAggregatorFunctionSupplier(List<Integer> channels, int precision) {
    this.channels = channels;
    this.precision = precision;
  }

  @Override
  public CountDistinctBytesRefAggregatorFunction aggregator(DriverContext driverContext) {
    return CountDistinctBytesRefAggregatorFunction.create(driverContext, channels, precision);
  }

  @Override
  public CountDistinctBytesRefGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return CountDistinctBytesRefGroupingAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of bytes";
  }
}
