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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link CountDistinctLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("aggstate", ElementType.UNKNOWN)  );

  private final HllStates.GroupingState state;

  private final List<Integer> channels;

  private final BigArrays bigArrays;

  private final int precision;

  public CountDistinctLongGroupingAggregatorFunction(List<Integer> channels,
      HllStates.GroupingState state, BigArrays bigArrays, int precision) {
    this.channels = channels;
    this.state = state;
    this.bigArrays = bigArrays;
    this.precision = precision;
  }

  public static CountDistinctLongGroupingAggregatorFunction create(List<Integer> channels,
      BigArrays bigArrays, int precision) {
    return new CountDistinctLongGroupingAggregatorFunction(channels, CountDistinctLongAggregator.initGrouping(bigArrays, precision), bigArrays, precision);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(LongVector groups, Page page) {
    assert groups.getPositionCount() == page.getPositionCount();
    Block uncastValuesBlock = page.getBlock(channels.get(0));
    if (uncastValuesBlock.areAllValuesNull()) {
      addRawInputAllNulls(groups, uncastValuesBlock);
      return;
    }
    LongBlock valuesBlock = (LongBlock) uncastValuesBlock;
    LongVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      addRawInput(groups, valuesBlock);
    } else {
      addRawInput(groups, valuesVector);
    }
  }

  private void addRawInput(LongVector groups, LongBlock values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      if (values.isNull(position)) {
        state.putNull(groupId);
        continue;
      }
      int valuesStart = values.getFirstValueIndex(position);
      int valuesEnd = valuesStart + values.getValueCount(position);
      for (int v = valuesStart; v < valuesEnd; v++) {
        CountDistinctLongAggregator.combine(state, groupId, values.getLong(v));
      }
    }
  }

  private void addRawInput(LongVector groups, LongVector values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      CountDistinctLongAggregator.combine(state, groupId, values.getLong(position));
    }
  }

  private void addRawInputAllNulls(LongVector groups, Block values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groups.getLong(position));
      assert values.isNull(position);
      state.putNull(groupId);
    }
  }

  @Override
  public void addRawInput(LongBlock groups, Page page) {
    assert groups.getPositionCount() == page.getPositionCount();
    Block uncastValuesBlock = page.getBlock(channels.get(0));
    if (uncastValuesBlock.areAllValuesNull()) {
      addRawInputAllNulls(groups, uncastValuesBlock);
      return;
    }
    LongBlock valuesBlock = (LongBlock) uncastValuesBlock;
    LongVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      addRawInput(groups, valuesBlock);
    } else {
      addRawInput(groups, valuesVector);
    }
  }

  private void addRawInput(LongBlock groups, LongBlock values) {
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
          CountDistinctLongAggregator.combine(state, groupId, values.getLong(v));
        }
      }
    }
  }

  private void addRawInput(LongBlock groups, LongVector values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      if (groups.isNull(position)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(position);
      int groupEnd = groupStart + groups.getValueCount(position);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        CountDistinctLongAggregator.combine(state, groupId, values.getLong(position));
      }
    }
  }

  private void addRawInputAllNulls(LongBlock groups, Block values) {
    for (int position = 0; position < groups.getPositionCount(); position++) {
      if (groups.isNull(position)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(position);
      int groupEnd = groupStart + groups.getValueCount(position);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getLong(g));
        assert values.isNull(position);
        state.putNull(groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(LongVector groupIdVector, Page page) {
    Block block = page.getBlock(channels.get(0));
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<HllStates.GroupingState> blobVector = (AggregatorStateVector<HllStates.GroupingState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    HllStates.GroupingState inState = CountDistinctLongAggregator.initGrouping(bigArrays, precision);
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      CountDistinctLongAggregator.combineStates(state, groupId, inState, position);
    }
    inState.close();
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    HllStates.GroupingState inState = ((CountDistinctLongGroupingAggregatorFunction) input).state;
    CountDistinctLongAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    AggregatorStateVector.Builder<AggregatorStateVector<HllStates.GroupingState>, HllStates.GroupingState> builder =
        AggregatorStateVector.builderOfAggregatorState(HllStates.GroupingState.class, state.getEstimatedSize());
    builder.add(state, selected);
    blocks[offset] = builder.build().asBlock();
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected) {
    blocks[offset] = CountDistinctLongAggregator.evaluateFinal(state, selected);
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
