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
 * {@link GroupingAggregatorFunction} implementation for {@link AvgIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class AvgIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final AvgLongAggregator.GroupingAvgState state;

  private final int channel;

  public AvgIntGroupingAggregatorFunction(int channel, AvgLongAggregator.GroupingAvgState state) {
    this.channel = channel;
    this.state = state;
  }

  public static AvgIntGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new AvgIntGroupingAggregatorFunction(channel, AvgIntAggregator.initGrouping(bigArrays));
  }

  @Override
  public void addRawInput(LongVector groups, Page page) {
    IntBlock valuesBlock = page.getBlock(channel);
    IntVector valuesVector = valuesBlock.asVector();
    if (valuesVector != null) {
      int positions = groups.getPositionCount();
      for (int position = 0; position < groups.getPositionCount(); position++) {
        int groupId = Math.toIntExact(groups.getLong(position));
        AvgIntAggregator.combine(state, groupId, valuesVector.getInt(position));
      }
    } else {
      // move the cold branch out of this method to keep the optimized case vector/vector as small as possible
      addRawInputWithBlockValues(groups, valuesBlock);
    }
  }

  private void addRawInputWithBlockValues(LongVector groups, IntBlock valuesBlock) {
    int positions = groups.getPositionCount();
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      if (valuesBlock.isNull(position)) {
        state.putNull(groupId);
      } else {
        int i = valuesBlock.getFirstValueIndex(position);
        AvgIntAggregator.combine(state, groupId, valuesBlock.getInt(i));
      }
    }
  }

  @Override
  public void addRawInput(LongBlock groups, Page page) {
    assert channel >= 0;
    IntBlock valuesBlock = page.getBlock(channel);
    IntVector valuesVector = valuesBlock.asVector();
    int positions = groups.getPositionCount();
    if (valuesVector != null) {
      for (int position = 0; position < groups.getPositionCount(); position++) {
        if (groups.isNull(position) == false) {
          int groupId = Math.toIntExact(groups.getLong(position));
          AvgIntAggregator.combine(state, groupId, valuesVector.getInt(position));
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
          int i = valuesBlock.getFirstValueIndex(position);
          AvgIntAggregator.combine(state, groupId, valuesBlock.getInt(position));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<AvgLongAggregator.GroupingAvgState> blobVector = (AggregatorStateVector<AvgLongAggregator.GroupingAvgState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    AvgLongAggregator.GroupingAvgState inState = AvgIntAggregator.initGrouping(bigArrays);
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      AvgIntAggregator.combineStates(state, groupId, inState, position);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    AvgLongAggregator.GroupingAvgState inState = ((AvgIntGroupingAggregatorFunction) input).state;
    AvgIntAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public Block evaluateIntermediate(IntVector selected) {
    AggregatorStateVector.Builder<AggregatorStateVector<AvgLongAggregator.GroupingAvgState>, AvgLongAggregator.GroupingAvgState> builder =
        AggregatorStateVector.builderOfAggregatorState(AvgLongAggregator.GroupingAvgState.class, state.getEstimatedSize());
    builder.add(state, selected);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal(IntVector selected) {
    return AvgIntAggregator.evaluateFinal(state, selected);
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
