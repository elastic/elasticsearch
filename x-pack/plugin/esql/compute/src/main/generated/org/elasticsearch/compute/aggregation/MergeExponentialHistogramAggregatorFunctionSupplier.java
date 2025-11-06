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
 * {@link AggregatorFunctionSupplier} implementation for {@link MergeExponentialHistogramAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MergeExponentialHistogramAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public MergeExponentialHistogramAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return MergeExponentialHistogramAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return MergeExponentialHistogramGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public MergeExponentialHistogramAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return MergeExponentialHistogramAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MergeExponentialHistogramGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return MergeExponentialHistogramGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "merge_exponential of histograms";
  }
}
