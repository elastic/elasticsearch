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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MaxDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final DoubleArrayState state;

  private final int channel;

  public MaxDoubleGroupingAggregatorFunction(int channel, DoubleArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MaxDoubleGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new MaxDoubleGroupingAggregatorFunction(channel, new DoubleArrayState(bigArrays, MaxDoubleAggregator.init()));
  }

  @Override
  public void addRawInput(LongVector groups, Page page) {
    DoubleBlock valuesBlock = page.getBlock(channel);
    DoubleVector valuesVector = valuesBlock.asVector();
    if (valuesVector != null) {
      int positions = groups.getPositionCount();
      for (int position = 0; position < groups.getPositionCount(); position++) {
        int groupId = Math.toIntExact(groups.getLong(position));
        state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), valuesVector.getDouble(position)), groupId);
      }
    } else {
      // move the cold branch out of this method to keep the optimized case vector/vector as small as possible
      addRawInputWithBlockValues(groups, valuesBlock);
    }
  }

  private void addRawInputWithBlockValues(LongVector groups, DoubleBlock valuesBlock) {
    int positions = groups.getPositionCount();
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      if (valuesBlock.isNull(position)) {
        state.putNull(groupId);
      } else {
        state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), valuesBlock.getDouble(position)), groupId);
      }
    }
  }

  @Override
  public void addRawInput(LongBlock groups, Page page) {
    assert channel >= 0;
    DoubleBlock valuesBlock = page.getBlock(channel);
    DoubleVector valuesVector = valuesBlock.asVector();
    int positions = groups.getPositionCount();
    if (valuesVector != null) {
      for (int position = 0; position < groups.getPositionCount(); position++) {
        if (groups.isNull(position) == false) {
          int groupId = Math.toIntExact(groups.getLong(position));
          state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), valuesVector.getDouble(position)), groupId);
        }
      }
    } else {
      for (int position = 0; position < groups.getPositionCount(); position++) {
        if (groups.isNull(position)) {
          continue;
        }
        int groupId = Math.toIntExact(groups.getLong(position));
        if (valuesBlock.isNull(position)) {
          state.putNull(groupId);
        } else {
          state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), valuesBlock.getDouble(position)), groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(LongVector groupIdVector, Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleArrayState> blobVector = (AggregatorStateVector<DoubleArrayState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    DoubleArrayState inState = new DoubleArrayState(bigArrays, MaxDoubleAggregator.init());
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    DoubleArrayState inState = ((MaxDoubleGroupingAggregatorFunction) input).state;
    state.set(MaxDoubleAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
  }

  @Override
  public Block evaluateIntermediate(IntVector selected) {
    AggregatorStateVector.Builder<AggregatorStateVector<DoubleArrayState>, DoubleArrayState> builder =
        AggregatorStateVector.builderOfAggregatorState(DoubleArrayState.class, state.getEstimatedSize());
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
