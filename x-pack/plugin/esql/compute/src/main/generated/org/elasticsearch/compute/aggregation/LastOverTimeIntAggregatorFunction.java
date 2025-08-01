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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link LastOverTimeIntAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class LastOverTimeIntAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.INT),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final LongIntState state;

  private final List<Integer> channels;

  public LastOverTimeIntAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      LongIntState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static LastOverTimeIntAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new LastOverTimeIntAggregatorFunction(driverContext, channels, LastOverTimeIntAggregator.initSingle(driverContext));
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
      IntBlock block = page.getBlock(channels.get(0));
      IntVector vector = block.asVector();
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
    IntBlock block = page.getBlock(channels.get(0));
    IntVector vector = block.asVector();
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

  private void addRawVector(IntVector vector, LongVector timestamps) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      LastOverTimeIntAggregator.combine(state, timestamps.getLong(i), vector.getInt(i));
    }
  }

  private void addRawVector(IntVector vector, LongVector timestamps, BooleanVector mask) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      LastOverTimeIntAggregator.combine(state, timestamps.getLong(i), vector.getInt(i));
    }
  }

  private void addRawBlock(IntBlock block, LongVector timestamps) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        LastOverTimeIntAggregator.combine(state, timestamps.getLong(i), block.getInt(i));
      }
    }
  }

  private void addRawBlock(IntBlock block, LongVector timestamps, BooleanVector mask) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        LastOverTimeIntAggregator.combine(state, timestamps.getLong(i), block.getInt(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      return;
    }
    LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
    assert timestamps.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    IntVector values = ((IntBlock) valuesUncast).asVector();
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    LastOverTimeIntAggregator.combineIntermediate(state, timestamps.getLong(0), values.getInt(0), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    if (state.seen() == false) {
      blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
      return;
    }
    blocks[offset] = LastOverTimeIntAggregator.evaluateFinal(state, driverContext);
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
