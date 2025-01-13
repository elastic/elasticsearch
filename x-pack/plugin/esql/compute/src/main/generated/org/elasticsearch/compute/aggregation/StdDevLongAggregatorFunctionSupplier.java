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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDevLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class StdDevLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public StdDevLongAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public StdDevLongAggregatorFunction aggregator(DriverContext driverContext) {
    return StdDevLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDevLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return StdDevLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_dev of longs";
  }
}
