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
 * {@link AggregatorFunctionSupplier} implementation for {@link ChangePointLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class ChangePointLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public ChangePointLongAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public ChangePointLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return ChangePointLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "change_point of longs";
  }
}
