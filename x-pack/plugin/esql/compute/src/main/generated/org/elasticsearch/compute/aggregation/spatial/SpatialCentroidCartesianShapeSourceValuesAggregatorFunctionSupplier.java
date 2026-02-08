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
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialCentroidCartesianShapeSourceValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialCentroidCartesianShapeSourceValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialCentroidCartesianShapeSourceValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialCentroidCartesianShapeSourceValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialCentroidCartesianShapeSourceValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialCentroidCartesianShapeSourceValuesAggregatorFunction aggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialCentroidCartesianShapeSourceValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialCentroidCartesianShapeSourceValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialCentroidCartesianShapeSourceValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_centroid_cartesian_shape_source of valuess";
  }
}
