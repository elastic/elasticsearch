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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialCentroidCartesianPointSourceValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier(
      List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public SpatialCentroidCartesianPointSourceValuesAggregatorFunction aggregator(
      DriverContext driverContext) {
    return SpatialCentroidCartesianPointSourceValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialCentroidCartesianPointSourceValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return SpatialCentroidCartesianPointSourceValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_centroid_cartesian_point_source of valuess";
  }
}
