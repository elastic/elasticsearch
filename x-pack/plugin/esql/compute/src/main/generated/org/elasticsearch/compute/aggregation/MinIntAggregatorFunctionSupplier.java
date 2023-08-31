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
 * {@link AggregatorFunctionSupplier} implementation for {@link MinIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final DriverContext driverContext;

  private final List<Integer> channels;

  public MinIntAggregatorFunctionSupplier(DriverContext driverContext, List<Integer> channels) {
    this.driverContext = driverContext;
    this.channels = channels;
  }

  @Override
  public MinIntAggregatorFunction aggregator() {
    return MinIntAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public MinIntGroupingAggregatorFunction groupingAggregator() {
    return MinIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min of ints";
  }
}
