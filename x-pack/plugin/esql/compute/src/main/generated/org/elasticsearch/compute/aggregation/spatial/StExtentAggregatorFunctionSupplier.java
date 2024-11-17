// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link StExtentAggregator}.
 * This class is generated. Do not edit it.
 */
public final class StExtentAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public StExtentAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public StExtentAggregatorFunction aggregator(DriverContext driverContext) {
    return StExtentAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StExtentGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return StExtentGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "st of extents";
  }
}
