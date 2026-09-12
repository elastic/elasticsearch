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
 * {@link AggregatorFunctionSupplier} implementation for {@link PercentileDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class PercentileDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  private final double percentile;

  private final double tDigestStateCompression;

  public PercentileDoubleAggregatorFunctionSupplier(WarningSourceLocation warningsSource,
      double percentile, double tDigestStateCompression) {
    this.warningsSource = warningsSource;
    this.percentile = percentile;
    this.tDigestStateCompression = tDigestStateCompression;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return PercentileDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return PercentileDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public PercentileDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new PercentileDoubleAggregatorFunction(warnings, driverContext, channels, percentile, tDigestStateCompression);
  }

  @Override
  public PercentileDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new PercentileDoubleGroupingAggregatorFunction(warnings, channels, driverContext, percentile, tDigestStateCompression);
  }

  @Override
  public String describe() {
    return "percentile of doubles";
  }
}
