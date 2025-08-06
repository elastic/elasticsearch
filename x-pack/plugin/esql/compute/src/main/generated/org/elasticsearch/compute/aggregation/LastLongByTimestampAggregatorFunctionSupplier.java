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
 * {@link AggregatorFunctionSupplier} implementation for {@link LastLongByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class LastLongByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public LastLongByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return LastLongByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public LastLongByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return LastLongByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "last_long_by of timestamps";
  }
}
