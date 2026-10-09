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

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link PromqlHistogramFractionAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PromqlHistogramFractionAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  private final double lower;

  private final double upper;

  public PromqlHistogramFractionAggregatorFunctionSupplier(WarningSourceLocation warningsSource,
      double lower, double upper) {
    this.warningsSource = warningsSource;
    this.lower = lower;
    this.upper = upper;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PromqlHistogramFractionAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PromqlHistogramFractionGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PromqlHistogramFractionAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = driverContext.createWarnings(warningsSource);
    return new PromqlHistogramFractionAggregatorFunction(warnings, driverContext, channels, lower, upper);
  }

  @Override
  public PromqlHistogramFractionGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    var warnings = driverContext.createWarnings(warningsSource);
    return new PromqlHistogramFractionGroupingAggregatorFunction(warnings, channels, driverContext, lower, upper);
  }

  @Override
  public String describe() {
    return PromqlHistogramFractionAggregator.describe();
  }
}
