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
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  int warningsLineNumber;

  int warningsColumnNumber;

  String warningsSourceText;

  private final List<Integer> channels;

  public SumLongAggregatorFunctionSupplier(int warningsLineNumber, int warningsColumnNumber,
      String warningsSourceText, List<Integer> channels) {
    this.warningsLineNumber = warningsLineNumber;
    this.warningsColumnNumber = warningsColumnNumber;
    this.warningsSourceText = warningsSourceText;
    this.channels = channels;
  }

  @Override
  public SumLongAggregatorFunction aggregator(DriverContext driverContext) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsLineNumber, warningsColumnNumber, warningsSourceText);
    return SumLongAggregatorFunction.create(warnings, driverContext, channels);
  }

  @Override
  public SumLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
    var warnings = Warnings.createWarnings(driverContext.warningsMode(), warningsLineNumber, warningsColumnNumber, warningsSourceText);
    return SumLongGroupingAggregatorFunction.create(warnings, channels, driverContext);
  }

  @Override
  public String describe() {
    return "sum of longs";
  }
}
