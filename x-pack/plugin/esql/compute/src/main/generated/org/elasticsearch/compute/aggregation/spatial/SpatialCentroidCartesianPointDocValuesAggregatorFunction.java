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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialCentroidCartesianPointDocValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialCentroidCartesianPointDocValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("count", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final CentroidPointAggregator.CentroidState state;

  private final List<Integer> channels;

  public SpatialCentroidCartesianPointDocValuesAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, CentroidPointAggregator.CentroidState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static SpatialCentroidCartesianPointDocValuesAggregatorFunction create(
      DriverContext driverContext, List<Integer> channels) {
    return new SpatialCentroidCartesianPointDocValuesAggregatorFunction(driverContext, channels, SpatialCentroidCartesianPointDocValuesAggregator.initSingle());
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
      return;
    }
    if (mask.allTrue()) {
      // No masking
      LongBlock block = page.getBlock(channels.get(0));
      LongVector vector = block.asVector();
      if (vector != null) {
        addRawVector(vector);
      } else {
        addRawBlock(block);
      }
      return;
    }
    // Some positions masked away, others kept
    LongBlock block = page.getBlock(channels.get(0));
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector, mask);
    } else {
      addRawBlock(block, mask);
    }
  }

  private void addRawVector(LongVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      SpatialCentroidCartesianPointDocValuesAggregator.combine(state, vector.getLong(i));
    }
  }

  private void addRawVector(LongVector vector, BooleanVector mask) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      SpatialCentroidCartesianPointDocValuesAggregator.combine(state, vector.getLong(i));
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        SpatialCentroidCartesianPointDocValuesAggregator.combine(state, block.getLong(i));
      }
    }
  }

  private void addRawBlock(LongBlock block, BooleanVector mask) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        SpatialCentroidCartesianPointDocValuesAggregator.combine(state, block.getLong(i));
      }
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
    Block countUncast = page.getBlock(channels.get(4));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    assert count.getPositionCount() == 1;
    SpatialCentroidCartesianPointDocValuesAggregator.combineIntermediate(state, xVal.getDouble(0), xDel.getDouble(0), yVal.getDouble(0), yDel.getDouble(0), count.getLong(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialCentroidCartesianPointDocValuesAggregator.evaluateFinal(state, driverContext);
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
