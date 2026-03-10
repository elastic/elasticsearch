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
 * {@link AggregatorFunctionSupplier} implementation for {@link SumLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SumLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  WarningSourceLocation warningsSource;

  public SumLongAggregatorFunctionSupplier(WarningSourceLocation warningsSource) {
    this.warningsSource = warningsSource;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SumLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SumLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SumLongAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return SumLongAggregatorFunction.create(warnings, driverContext, channels);
  }

  @Override
  public SumLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsSource);
    return SumLongGroupingAggregatorFunction.create(warnings, channels, driverContext);
  }

  @Override
  public String describe() {
    return "sum of longs";
  }
}
