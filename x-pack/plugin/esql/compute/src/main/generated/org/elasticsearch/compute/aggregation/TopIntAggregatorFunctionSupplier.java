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
 * {@link AggregatorFunctionSupplier} implementation for {@link TopIntAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class TopIntAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  private final boolean ascending;

  public TopIntAggregatorFunctionSupplier(int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return TopIntAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return TopIntGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public TopIntAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
    return TopIntAggregatorFunction.create(driverContext, channels, limit, ascending);
  }

  @Override
  public TopIntGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    return TopIntGroupingAggregatorFunction.create(channels, driverContext, limit, ascending);
  }

  @Override
  public String describe() {
    return "top of ints";
  }
}
