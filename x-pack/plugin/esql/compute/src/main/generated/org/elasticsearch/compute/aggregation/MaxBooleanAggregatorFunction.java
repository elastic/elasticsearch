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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link MaxBooleanAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxBooleanAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("max", ElementType.BOOLEAN),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final BooleanState state;

  private final List<Integer> channels;

  public MaxBooleanAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      BooleanState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static MaxBooleanAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new MaxBooleanAggregatorFunction(driverContext, channels, new BooleanState(MaxBooleanAggregator.init()));
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page) {
    BooleanBlock block = page.getBlock(channels.get(0));
    BooleanVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(BooleanVector vector) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.booleanValue(MaxBooleanAggregator.combine(state.booleanValue(), vector.getBoolean(i)));
    }
  }

  private void addRawBlock(BooleanBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        state.booleanValue(MaxBooleanAggregator.combine(state.booleanValue(), block.getBoolean(i)));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block maxUncast = page.getBlock(channels.get(0));
    if (maxUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector max = ((BooleanBlock) maxUncast).asVector();
    assert max.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    if (seen.getBoolean(0)) {
      state.booleanValue(MaxBooleanAggregator.combine(state.booleanValue(), max.getBoolean(0)));
      state.seen(true);
    }
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
    blocks[offset] = driverContext.blockFactory().newConstantBooleanBlockWith(state.booleanValue(), 1);
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
