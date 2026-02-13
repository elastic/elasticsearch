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
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MaxBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MaxBytesRefAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MaxBytesRefAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MaxBytesRefGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MaxBytesRefAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MaxBytesRefAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MaxBytesRefGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MaxBytesRefGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "max of bytes";
  }
}
