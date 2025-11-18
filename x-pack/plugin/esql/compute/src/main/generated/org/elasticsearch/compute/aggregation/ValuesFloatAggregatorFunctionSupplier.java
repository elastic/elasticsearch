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
 * {@link AggregatorFunctionSupplier} implementation for {@link ValuesFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class ValuesFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public ValuesFloatAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return ValuesFloatAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return ValuesFloatGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public ValuesFloatAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return ValuesFloatAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public ValuesFloatGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return ValuesFloatGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "values of floats";
  }
}
