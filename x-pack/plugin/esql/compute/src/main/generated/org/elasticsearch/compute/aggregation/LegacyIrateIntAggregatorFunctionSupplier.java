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
 * {@link AggregatorFunctionSupplier} implementation for {@link LegacyIrateIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class LegacyIrateIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  private final boolean isDateNanos;

  public LegacyIrateIntAggregatorFunctionSupplier(WarningSourceLocation warningsSource,
      boolean isDateNanos) {
    this.warningsSource = warningsSource;
    this.isDateNanos = isDateNanos;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return LegacyIrateIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    throw new UnsupportedOperationException("non-grouping aggregator is not supported");
  }

  @Override
  public LegacyIrateIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return new LegacyIrateIntGroupingAggregatorFunction(warnings, channels, driverContext, isDateNanos);
  }

  @Override
  public String describe() {
    return LegacyIrateIntAggregator.describe();
  }
}
