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
 * {@link AggregatorFunctionSupplier} implementation for {@link SparklineBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SparklineBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SparklineBytesRefAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SparklineBytesRefAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SparklineBytesRefGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SparklineBytesRefAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SparklineBytesRefAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SparklineBytesRefGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SparklineBytesRefGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "sparkline of bytes";
  }
}
