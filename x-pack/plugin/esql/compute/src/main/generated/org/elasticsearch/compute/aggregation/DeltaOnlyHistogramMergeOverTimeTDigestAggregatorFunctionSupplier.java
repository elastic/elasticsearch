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
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link DeltaOnlyHistogramMergeOverTimeTDigestAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class DeltaOnlyHistogramMergeOverTimeTDigestAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  public DeltaOnlyHistogramMergeOverTimeTDigestAggregatorFunctionSupplier(
      WarningSourceLocation warningsSource) {
    this.warningsSource = warningsSource;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return DeltaOnlyHistogramMergeOverTimeTDigestGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public DeltaOnlyHistogramMergeOverTimeTDigestGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new DeltaOnlyHistogramMergeOverTimeTDigestGroupingAggregatorFunction(warnings, channels, driverContext);
  }

  @Override
  public String describe() {
    return "delta_only_histogram_merge_over_time of tdigests";
  }
}
