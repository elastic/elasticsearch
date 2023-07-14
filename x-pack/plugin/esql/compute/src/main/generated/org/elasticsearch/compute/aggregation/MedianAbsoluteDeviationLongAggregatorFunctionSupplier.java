// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link MedianAbsoluteDeviationLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MedianAbsoluteDeviationLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  public MedianAbsoluteDeviationLongAggregatorFunctionSupplier(BigArrays bigArrays,
      List<Integer> channels) {
    this.bigArrays = bigArrays;
    this.channels = channels;
  }

  @Override
  public MedianAbsoluteDeviationLongAggregatorFunction aggregator() {
    return MedianAbsoluteDeviationLongAggregatorFunction.create(channels);
  }

  @Override
  public MedianAbsoluteDeviationLongGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return MedianAbsoluteDeviationLongGroupingAggregatorFunction.create(channels, driverContext, bigArrays);
  }

  @Override
  public String describe() {
    return "median_absolute_deviation of longs";
  }
}
