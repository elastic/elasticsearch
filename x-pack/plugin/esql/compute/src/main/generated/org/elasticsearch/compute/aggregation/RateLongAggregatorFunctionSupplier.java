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
 * {@link AggregatorFunctionSupplier} implementation for {@link RateLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class RateLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  private final long unitInMillis;

  public RateLongAggregatorFunctionSupplier(List<Integer> channels, long unitInMillis) {
    this.channels = channels;
    this.unitInMillis = unitInMillis;
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public RateLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return RateLongGroupingAggregatorFunction.create(channels, driverContext, unitInMillis);
  }

  @Override
  public String describe() {
    return "rate of longs";
  }
}
