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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link FirstOverTimeFloatAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class FirstOverTimeFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.FLOAT),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final LongFloatState state;

  private final List<Integer> channels;

  public FirstOverTimeFloatAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      LongFloatState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static FirstOverTimeFloatAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new FirstOverTimeFloatAggregatorFunction(driverContext, channels, FirstOverTimeFloatAggregator.initSingle(driverContext));
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
      FloatBlock block = page.getBlock(channels.get(0));
      FloatVector vector = block.asVector();
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
    FloatBlock block = page.getBlock(channels.get(0));
    FloatVector vector = block.asVector();
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

  private void addRawVector(FloatVector vector, LongVector timestamps) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      FirstOverTimeFloatAggregator.combine(state, timestamps.getLong(i), vector.getFloat(i));
    }
  }

  private void addRawVector(FloatVector vector, LongVector timestamps, BooleanVector mask) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      FirstOverTimeFloatAggregator.combine(state, timestamps.getLong(i), vector.getFloat(i));
    }
  }

  private void addRawBlock(FloatBlock block, LongVector timestamps) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        FirstOverTimeFloatAggregator.combine(state, timestamps.getLong(i), block.getFloat(i));
      }
    }
  }

  private void addRawBlock(FloatBlock block, LongVector timestamps, BooleanVector mask) {
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
        FirstOverTimeFloatAggregator.combine(state, timestamps.getLong(i), block.getFloat(i));
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
    FloatVector values = ((FloatBlock) valuesUncast).asVector();
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    FirstOverTimeFloatAggregator.combineIntermediate(state, timestamps.getLong(0), values.getFloat(0), seen.getBoolean(0));
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
    blocks[offset] = FirstOverTimeFloatAggregator.evaluateFinal(state, driverContext);
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
