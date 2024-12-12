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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentGeoShapeAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialExtentGeoShapeAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public SpatialExtentGeoShapeAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public SpatialExtentGeoShapeAggregatorFunction aggregator(DriverContext driverContext) {
    return SpatialExtentGeoShapeAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialExtentGeoShapeGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return SpatialExtentGeoShapeGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_geo of shapes";
  }
}
