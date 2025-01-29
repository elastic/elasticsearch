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
 * {@link AggregatorFunctionSupplier} implementation for {@link MedianAbsoluteDeviationFloatAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class MedianAbsoluteDeviationFloatAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public MedianAbsoluteDeviationFloatAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public MedianAbsoluteDeviationFloatAggregatorFunction aggregator(DriverContext driverContext) {
    return MedianAbsoluteDeviationFloatAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public MedianAbsoluteDeviationFloatGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return MedianAbsoluteDeviationFloatGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "median_absolute_deviation of floats";
  }
}
