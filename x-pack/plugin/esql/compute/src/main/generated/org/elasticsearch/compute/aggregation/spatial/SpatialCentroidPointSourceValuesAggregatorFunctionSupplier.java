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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialCentroidPointSourceValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialCentroidPointSourceValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final CoordinateEncoder encoder;

  public SpatialCentroidPointSourceValuesAggregatorFunctionSupplier(CoordinateEncoder encoder) {
    this.encoder = encoder;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialCentroidPointSourceValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialCentroidPointSourceValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialCentroidPointSourceValuesAggregatorFunction aggregator(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    return new SpatialCentroidPointSourceValuesAggregatorFunction(driverContext, inputs, encoder);
  }

  @Override
  public SpatialCentroidPointSourceValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialCentroidPointSourceValuesGroupingAggregatorFunction(channels, driverContext, encoder);
  }

  @Override
  public String describe() {
    return "spatial_centroid_point_source of valuess";
  }
}
