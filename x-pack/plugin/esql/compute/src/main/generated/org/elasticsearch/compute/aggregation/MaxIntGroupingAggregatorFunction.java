// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MaxIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final IntArrayState state;

  private final int channel;

  public MaxIntGroupingAggregatorFunction(int channel, IntArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MaxIntGroupingAggregatorFunction create(int channel, BigArrays bigArrays) {
    return new MaxIntGroupingAggregatorFunction(channel, new IntArrayState(bigArrays, MaxIntAggregator.init()));
  }

  @Override
  public void addRawInput(LongVector groups, Page page) {
    IntBlock valuesBlock = page.getBlock(channel);
    assert groups.getPositionCount() == page.getPositionCount();
    IntVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      addRawInput(groups, valuesBlock);
    } else {
      addRawInput(groups, valuesVector);
    }
  }

  private void addRawInput(LongVector groups, IntBlock values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      if (values.isNull(position)) {
        state.putNull(groupId);
        continue;
      }
      int valuesStart = values.getFirstValueIndex(position);
      int valuesEnd = valuesStart + values.getValueCount(position);
      for (int v = valuesStart; v < valuesEnd; v++) {
        state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), values.getInt(v)), groupId);
      }
    }
  }

  private void addRawInput(LongVector groups, IntVector values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), values.getInt(position)), groupId);
    }
  }

  @Override
  public void addRawInput(LongBlock groups, Page page) {
    IntBlock valuesBlock = page.getBlock(channel);
    assert groups.getPositionCount() == page.getPositionCount();
    IntVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      addRawInput(groups, valuesBlock);
    } else {
      addRawInput(groups, valuesVector);
    }
  }

  private void addRawInput(LongBlock groups, IntBlock values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      if (groups.isNull(position)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(position);
      int groupEnd = groupStart + groups.getValueCount(position);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        if (values.isNull(position)) {
          state.putNull(groupId);
          continue;
        }
        int valuesStart = values.getFirstValueIndex(position);
        int valuesEnd = valuesStart + values.getValueCount(position);
        for (int v = valuesStart; v < valuesEnd; v++) {
          state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), values.getInt(v)), groupId);
        }
      }
    }
  }

  private void addRawInput(LongBlock groups, IntVector values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      if (groups.isNull(position)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(position);
      int groupEnd = groupStart + groups.getValueCount(position);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), values.getInt(position)), groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(LongVector groupIdVector, Block block) {
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<IntArrayState> blobVector = (AggregatorStateVector<IntArrayState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    IntArrayState inState = new IntArrayState(bigArrays, MaxIntAggregator.init());
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
    }
    inState.close();
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    IntArrayState inState = ((MaxIntGroupingAggregatorFunction) input).state;
    state.set(MaxIntAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
  }

  @Override
  public Block evaluateIntermediate(IntVector selected) {
    AggregatorStateVector.Builder<AggregatorStateVector<IntArrayState>, IntArrayState> builder =
        AggregatorStateVector.builderOfAggregatorState(IntArrayState.class, state.getEstimatedSize());
    builder.add(state, selected);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal(IntVector selected) {
    return state.toValuesBlock(selected);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channel=").append(channel);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
