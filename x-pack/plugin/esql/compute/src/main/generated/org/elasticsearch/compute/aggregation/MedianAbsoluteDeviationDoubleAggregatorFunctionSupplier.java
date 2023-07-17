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

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link MedianAbsoluteDeviationDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final BigArrays bigArrays;

  private final List<Integer> channels;

  public MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier(BigArrays bigArrays,
      List<Integer> channels) {
    this.bigArrays = bigArrays;
    this.channels = channels;
  }

  @Override
  public MedianAbsoluteDeviationDoubleAggregatorFunction aggregator() {
    return MedianAbsoluteDeviationDoubleAggregatorFunction.create(channels);
  }

  @Override
  public MedianAbsoluteDeviationDoubleGroupingAggregatorFunction groupingAggregator() {
    return MedianAbsoluteDeviationDoubleGroupingAggregatorFunction.create(channels, bigArrays);
  }

  @Override
  public String describe() {
    return "median_absolute_deviation of doubles";
  }
}
