// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link AnyBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class AnyBytesRefAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public AnyBytesRefAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return AnyBytesRefAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return AnyBytesRefGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public AnyBytesRefAggregatorFunction aggregator(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    return new AnyBytesRefAggregatorFunction(driverContext, inputs);
  }

  @Override
  public AnyBytesRefGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyBytesRefGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return AnyBytesRefAggregator.describe();
  }
}
