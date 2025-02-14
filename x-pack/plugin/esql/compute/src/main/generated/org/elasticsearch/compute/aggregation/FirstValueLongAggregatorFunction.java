// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link FirstValueLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class FirstValueLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.LONG),
      new IntermediateStateDesc("timestamp", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final FirstValueLongAggregator.FirstValueLongSingleState state;

  private final List<Integer> channels;

  public FirstValueLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      FirstValueLongAggregator.FirstValueLongSingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static FirstValueLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new FirstValueLongAggregatorFunction(driverContext, channels, FirstValueLongAggregator.initSingle());
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
      LongBlock timestampsBlock = page.getBlock(channels.get(1));
      LongVector timestampsVector = timestampsBlock.asVector();
      if (timestampsVector == null)  {
        throw new IllegalStateException("expected @timestamp vector; but got a block");
      }
      if (vector != null) {
        addRawVector(vector, timestampsVector);
      } else {
        addRawBlock(block, timestampsVector);
      }
      return;
    }
    // Some positions masked away, others kept
    LongBlock block = page.getBlock(channels.get(0));
    LongVector vector = block.asVector();
    LongBlock timestampsBlock = page.getBlock(channels.get(1));
    LongVector timestampsVector = timestampsBlock.asVector();
    if (timestampsVector == null)  {
      throw new IllegalStateException("expected @timestamp vector; but got a block");
    }
    if (vector != null) {
      addRawVector(vector, timestampsVector, mask);
    } else {
      addRawBlock(block, timestampsVector, mask);
    }
  }

  private void addRawVector(LongVector vector, LongVector timestamps) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      FirstValueLongAggregator.combine(state, timestamps.getLong(i), vector.getLong(i));
    }
  }

  private void addRawVector(LongVector vector, LongVector timestamps, BooleanVector mask) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      FirstValueLongAggregator.combine(state, timestamps.getLong(i), vector.getLong(i));
    }
  }

  private void addRawBlock(LongBlock block, LongVector timestamps) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        FirstValueLongAggregator.combine(state, timestamps.getLong(i), block.getLong(i));
      }
    }
  }

  private void addRawBlock(LongBlock block, LongVector timestamps, BooleanVector mask) {
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
        FirstValueLongAggregator.combine(state, timestamps.getLong(i), block.getLong(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    LongVector value = ((LongBlock) valueUncast).asVector();
    assert value.getPositionCount() == 1;
    Block timestampUncast = page.getBlock(channels.get(1));
    if (timestampUncast.areAllValuesNull()) {
      return;
    }
    LongVector timestamp = ((LongBlock) timestampUncast).asVector();
    assert timestamp.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    FirstValueLongAggregator.combineIntermediate(state, value.getLong(0), timestamp.getLong(0), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = FirstValueLongAggregator.evaluateFinal(state, driverContext);
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
