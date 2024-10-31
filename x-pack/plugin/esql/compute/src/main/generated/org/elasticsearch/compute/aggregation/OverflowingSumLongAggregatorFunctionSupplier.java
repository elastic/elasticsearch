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
 * {@link AggregatorFunctionSupplier} implementation for {@link OverflowingSumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class OverflowingSumLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public OverflowingSumLongAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public OverflowingSumLongAggregatorFunction aggregator(DriverContext driverContext) {
    return OverflowingSumLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public OverflowingSumLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return OverflowingSumLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "overflowing_sum of longs";
  }
}
