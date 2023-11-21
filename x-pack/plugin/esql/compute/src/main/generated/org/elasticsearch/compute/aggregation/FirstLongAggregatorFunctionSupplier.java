// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link FirstLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class FirstLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  public FirstLongAggregatorFunctionSupplier(BigArrays bigArrays, List<Integer> channels) {
    this.bigArrays = bigArrays;
    this.channels = channels;
  }

  @Override
  public FirstLongAggregatorFunction aggregator(DriverContext driverContext) {
    return FirstLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public FirstLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    return FirstLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "first of longs";
  }
}
