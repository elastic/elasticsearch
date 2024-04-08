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
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public MaxIntAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public MaxIntAggregatorFunction aggregator(DriverContext driverContext) {
    return MaxIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MaxIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return MaxIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "max of ints";
  }
}
