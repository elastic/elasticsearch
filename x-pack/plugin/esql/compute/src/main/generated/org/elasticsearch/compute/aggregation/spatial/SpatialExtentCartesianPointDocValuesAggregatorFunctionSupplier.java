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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentCartesianPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialExtentCartesianPointDocValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialExtentCartesianPointDocValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialExtentCartesianPointDocValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialExtentCartesianPointDocValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialExtentCartesianPointDocValuesAggregatorFunction aggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialExtentCartesianPointDocValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialExtentCartesianPointDocValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialExtentCartesianPointDocValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_cartesian_point_doc of valuess";
  }
}
