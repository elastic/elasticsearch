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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentGeoPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialExtentGeoPointDocValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialExtentGeoPointDocValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialExtentGeoPointDocValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialExtentGeoPointDocValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialExtentGeoPointDocValuesAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    return SpatialExtentGeoPointDocValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialExtentGeoPointDocValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return SpatialExtentGeoPointDocValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_geo_point_doc of valuess";
  }
}
