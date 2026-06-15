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
 * {@link AggregatorFunctionSupplier} implementation for {@link PromqlHistogramQuantileAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PromqlHistogramQuantileAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  private final double quantile;

  public PromqlHistogramQuantileAggregatorFunctionSupplier(WarningSourceLocation warningsSource,
      double quantile) {
    this.warningsSource = warningsSource;
    this.quantile = quantile;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PromqlHistogramQuantileAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PromqlHistogramQuantileGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PromqlHistogramQuantileAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new PromqlHistogramQuantileAggregatorFunction(warnings, driverContext, channels, quantile);
  }

  @Override
  public PromqlHistogramQuantileGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new PromqlHistogramQuantileGroupingAggregatorFunction(warnings, channels, driverContext, quantile);
  }

  @Override
  public String describe() {
    return PromqlHistogramQuantileAggregator.describe();
  }
}
