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
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialCentroidCartesianPointDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final CoordinateEncoder encoder;

  public SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier(
      CoordinateEncoder encoder) {
    this.encoder = encoder;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialCentroidCartesianPointDocValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialCentroidCartesianPointDocValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialCentroidCartesianPointDocValuesAggregatorFunction aggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialCentroidCartesianPointDocValuesAggregatorFunction(driverContext, channels, encoder);
  }

  @Override
  public SpatialCentroidCartesianPointDocValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialCentroidCartesianPointDocValuesGroupingAggregatorFunction(channels, driverContext, encoder);
  }

  @Override
  public String describe() {
    return "spatial_centroid_cartesian_point_doc of valuess";
  }
}
