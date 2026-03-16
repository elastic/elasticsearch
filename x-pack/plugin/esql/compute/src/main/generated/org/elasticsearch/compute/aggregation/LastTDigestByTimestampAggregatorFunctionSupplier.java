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
 * {@link AggregatorFunctionSupplier} implementation for {@link LastTDigestByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class LastTDigestByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public LastTDigestByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return LastTDigestByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return LastTDigestByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public LastTDigestByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new LastTDigestByTimestampAggregatorFunction(driverContext, channels);
  }

  @Override
  public LastTDigestByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new LastTDigestByTimestampGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return LastTDigestByTimestampAggregator.describe();
  }
}
