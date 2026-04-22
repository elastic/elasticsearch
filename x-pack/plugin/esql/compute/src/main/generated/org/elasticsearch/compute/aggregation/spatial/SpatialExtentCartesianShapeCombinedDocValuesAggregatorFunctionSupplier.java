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

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SpatialExtentCartesianShapeCombinedDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SpatialExtentCartesianShapeCombinedDocValuesAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  public SpatialExtentCartesianShapeCombinedDocValuesAggregatorFunctionSupplier() {
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SpatialExtentCartesianShapeCombinedDocValuesAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SpatialExtentCartesianShapeCombinedDocValuesGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SpatialExtentCartesianShapeCombinedDocValuesAggregatorFunction aggregator(
      DriverContext driverContext, List<ExpressionEvaluator> inputs) {
    return new SpatialExtentCartesianShapeCombinedDocValuesAggregatorFunction(driverContext, inputs);
  }

  @Override
  public SpatialExtentCartesianShapeCombinedDocValuesGroupingAggregatorFunction groupingAggregator(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialExtentCartesianShapeCombinedDocValuesGroupingAggregatorFunction(channels, driverContext);
  }

  @Override
  public String describe() {
    return "spatial_extent_cartesian_shape_combined_doc of valuess";
  }
}
