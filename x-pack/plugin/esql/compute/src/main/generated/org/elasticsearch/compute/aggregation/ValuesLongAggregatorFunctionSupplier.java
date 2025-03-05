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
 * {@link AggregatorFunctionSupplier} implementation for {@link ValuesLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class ValuesLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public ValuesLongAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return ValuesLongAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return ValuesLongGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public ValuesLongAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return ValuesLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public ValuesLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return ValuesLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "values of longs";
  }
}
