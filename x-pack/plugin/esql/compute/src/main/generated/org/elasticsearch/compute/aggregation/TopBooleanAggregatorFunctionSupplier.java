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
 * {@link AggregatorFunctionSupplier} implementation for {@link TopBooleanAggregator}.
 * This class is generated. Do not edit it.
 */
public final class TopBooleanAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopBooleanAggregatorFunctionSupplier(List<Integer> channels, int limit,
      boolean ascending) {
    this.channels = channels;
    this.limit = limit;
    this.ascending = ascending;
  }

  @Override
  public TopBooleanAggregatorFunction aggregator(DriverContext driverContext) {
    return TopBooleanAggregatorFunction.create(driverContext, channels, limit, ascending);
  }

  @Override
  public TopBooleanGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return TopBooleanGroupingAggregatorFunction.create(channels, driverContext, limit, ascending);
  }

  @Override
  public String describe() {
    return "top of booleans";
  }
}
