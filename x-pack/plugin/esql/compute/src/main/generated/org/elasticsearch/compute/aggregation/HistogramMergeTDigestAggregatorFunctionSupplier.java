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
 * {@link AggregatorFunctionSupplier} implementation for {@link HistogramMergeTDigestAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class HistogramMergeTDigestAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public HistogramMergeTDigestAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return HistogramMergeTDigestAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return HistogramMergeTDigestGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public HistogramMergeTDigestAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return HistogramMergeTDigestAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public HistogramMergeTDigestGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return HistogramMergeTDigestGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "histogram_merge_t of digests";
  }
}
