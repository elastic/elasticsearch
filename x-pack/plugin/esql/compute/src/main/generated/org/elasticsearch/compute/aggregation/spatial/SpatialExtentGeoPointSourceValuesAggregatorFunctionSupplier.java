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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentGeoPointSourceValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialExtentGeoPointSourceValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialExtentGeoPointSourceValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialExtentGeoPointSourceValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialExtentGeoPointSourceValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialExtentGeoPointSourceValuesAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SpatialExtentGeoPointSourceValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialExtentGeoPointSourceValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialExtentGeoPointSourceValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_geo_point_source of valuess";
  }
}
