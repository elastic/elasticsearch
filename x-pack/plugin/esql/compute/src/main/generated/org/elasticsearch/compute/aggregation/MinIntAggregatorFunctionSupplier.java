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
public final class MinIntAggregatorFunctionSupplier extends AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public MinIntAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  protected MinIntAggregatorFunction aggregator(DriverContext driverContext) {
    return MinIntAggregatorFunction.create(driverContext, channels);
  }

  @Override
  protected MinIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return MinIntGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "min of ints";
  }
}
