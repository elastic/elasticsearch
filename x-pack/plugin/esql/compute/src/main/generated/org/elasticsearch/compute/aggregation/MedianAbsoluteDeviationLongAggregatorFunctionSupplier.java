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
 * {@link AggregatorFunctionSupplier} implementation for {@link MedianAbsoluteDeviationLongAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MedianAbsoluteDeviationLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public MedianAbsoluteDeviationLongAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public MedianAbsoluteDeviationLongAggregatorFunction aggregator(DriverContext driverContext) {
    return MedianAbsoluteDeviationLongAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MedianAbsoluteDeviationLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return MedianAbsoluteDeviationLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "median_absolute_deviation of longs";
  }
}
