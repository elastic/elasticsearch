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
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final DriverContext driverContext;

  private final List<Integer> channels;

  public MaxLongAggregatorFunctionSupplier(DriverContext driverContext, List<Integer> channels) {
    this.driverContext = driverContext;
    this.channels = channels;
  }

  @Override
  public MaxLongAggregatorFunction aggregator() {
    return MaxLongAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public MaxLongGroupingAggregatorFunction groupingAggregator() {
    return MaxLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "max of longs";
  }
}
