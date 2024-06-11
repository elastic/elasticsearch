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
 * {@link AggregatorFunctionSupplier} implementation for {@link TopValuesListIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class TopValuesListIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopValuesListIntAggregatorFunctionSupplier(List<Integer> channels, int limit,
      boolean ascending) {
    this.channels = channels;
    this.limit = limit;
    this.ascending = ascending;
  }

  @Override
  public TopValuesListIntAggregatorFunction aggregator(DriverContext driverContext) {
    return TopValuesListIntAggregatorFunction.create(driverContext, channels, limit, ascending);
  }

  @Override
  public TopValuesListIntGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return TopValuesListIntGroupingAggregatorFunction.create(channels, driverContext, limit, ascending);
  }

  @Override
  public String describe() {
    return "top_values_list of ints";
  }
}
