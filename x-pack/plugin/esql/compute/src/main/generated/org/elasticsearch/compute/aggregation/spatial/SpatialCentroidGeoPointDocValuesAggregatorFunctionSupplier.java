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
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialCentroidGeoPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final List<Integer> channels;

  public SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier(List<Integer> channels) {
    this.channels = channels;
  }

  @Override
  public SpatialCentroidGeoPointDocValuesAggregatorFunction aggregator(
      DriverContext driverContext) {
    return SpatialCentroidGeoPointDocValuesAggregatorFunction.create(driverContext, channels);
  }

  @Override
  public SpatialCentroidGeoPointDocValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext) {
    return SpatialCentroidGeoPointDocValuesGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_centroid_geo_point_doc of valuess";
  }
}
