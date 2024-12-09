// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialStExtentCartesianShapeAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialStExtentCartesianShapeAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public SpatialStExtentCartesianShapeAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public SpatialStExtentCartesianShapeAggregatorFunction aggregator(DriverContext driverContext) {
    return SpatialStExtentCartesianShapeAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialStExtentCartesianShapeGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return SpatialStExtentCartesianShapeGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_st_extent_cartesian of shapes";
  }
}
