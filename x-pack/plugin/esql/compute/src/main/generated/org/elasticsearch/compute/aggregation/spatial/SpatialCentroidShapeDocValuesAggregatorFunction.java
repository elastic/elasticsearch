// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialCentroidShapeDocValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialCentroidShapeDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("weight", ElementType.DOUBLE),
      new IntermediateStateDesc("shapeType", ElementType.INT)  );

  private final DriverContext driverContext;

  private final CentroidShapeAggregator.ShapeCentroidState state;

  private final List<Integer> channels;

  private final CoordinateEncoder encoder;

  SpatialCentroidShapeDocValuesAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, CoordinateEncoder encoder) {
    this.encoder = encoder;
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = SpatialCentroidShapeDocValuesAggregator.initSingle(encoder);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page, BooleanVector mask) {
    if (mask.allFalse()) {
      // Entire page masked away
    } else if (mask.allTrue()) {
      addRawInputNotMasked(page);
    } else {
      addRawInputMasked(page, mask);
    }
  }

  private void addRawInputMasked(Page page, BooleanVector mask) {
    DoubleBlock valuesBlock = page.getBlock(channels.get(0));
    addRawBlock(valuesBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    DoubleBlock valuesBlock = page.getBlock(channels.get(0));
    addRawBlock(valuesBlock);
  }

  private void addRawBlock(DoubleBlock valuesBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      SpatialCentroidShapeDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  private void addRawBlock(DoubleBlock valuesBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      SpatialCentroidShapeDocValuesAggregator.combine(state, p, valuesBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block xValUncast = page.getBlock(channels.get(0));
    if (xValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
    assert xVal.getPositionCount() == 1;
    Block xDelUncast = page.getBlock(channels.get(1));
    if (xDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
    assert xDel.getPositionCount() == 1;
    Block yValUncast = page.getBlock(channels.get(2));
    if (yValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
    assert yVal.getPositionCount() == 1;
    Block yDelUncast = page.getBlock(channels.get(3));
    if (yDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
    assert yDel.getPositionCount() == 1;
    Block weightUncast = page.getBlock(channels.get(4));
    if (weightUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector weight = ((DoubleBlock) weightUncast).asVector();
    assert weight.getPositionCount() == 1;
    Block shapeTypeUncast = page.getBlock(channels.get(5));
    if (shapeTypeUncast.areAllValuesNull()) {
      return;
    }
    IntVector shapeType = ((IntBlock) shapeTypeUncast).asVector();
    assert shapeType.getPositionCount() == 1;
    SpatialCentroidShapeDocValuesAggregator.combineIntermediate(state, xVal.getDouble(0), xDel.getDouble(0), yVal.getDouble(0), yDel.getDouble(0), weight.getDouble(0), shapeType.getInt(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialCentroidShapeDocValuesAggregator.evaluateFinal(state, driverContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channels=").append(channels);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
