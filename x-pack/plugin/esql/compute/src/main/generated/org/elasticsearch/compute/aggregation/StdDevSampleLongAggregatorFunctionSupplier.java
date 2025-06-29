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
 * {@link AggregatorFunctionSupplier} implementation for {@link StdDevSampleLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class StdDevSampleLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public StdDevSampleLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return StdDevSampleLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return StdDevSampleLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public StdDevSampleLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevSampleLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public StdDevSampleLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return StdDevSampleLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "std_dev_sample of longs";
  }
}
