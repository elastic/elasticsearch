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
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxIpAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MaxIpAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MaxIpAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MaxIpAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MaxIpGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MaxIpAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    return MaxIpAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MaxIpGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MaxIpGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "max of ips";
  }
}
