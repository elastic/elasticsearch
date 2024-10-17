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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDeviationLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class StdDeviationLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public StdDeviationLongAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public StdDeviationLongAggregatorFunction aggregator(DriverContext driverContext) {
    return StdDeviationLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDeviationLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return StdDeviationLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_deviation of longs";
  }
}
