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
 * {@link AggregatorFunctionSupplier} implementation for {@link FirstTDigestByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class FirstTDigestByTimestampAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public FirstTDigestByTimestampAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return FirstTDigestByTimestampAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return FirstTDigestByTimestampGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public FirstTDigestByTimestampAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return FirstTDigestByTimestampAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public FirstTDigestByTimestampGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return FirstTDigestByTimestampGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return FirstTDigestByTimestampAggregator.describe();
  }
}
