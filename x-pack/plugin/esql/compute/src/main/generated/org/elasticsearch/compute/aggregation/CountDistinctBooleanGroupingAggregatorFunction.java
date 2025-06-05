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
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link CountDistinctBooleanAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class CountDistinctBooleanGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("fbit", ElementType.BOOLEAN),
      new IntermediateStateDesc("tbit", ElementType.BOOLEAN)  );

  private final CountDistinctBooleanAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public CountDistinctBooleanGroupingAggregatorFunction(List<Integer> channels,
      CountDistinctBooleanAggregator.GroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static CountDistinctBooleanGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext) {
    return new CountDistinctBooleanGroupingAggregatorFunction(channels, CountDistinctBooleanAggregator.initGrouping(driverContext.bigArrays()), driverContext);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds,
      Page page) {
    BooleanBlock valuesBlock = page.getBlock(channels.get(0));
    BooleanVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      if (valuesBlock.mayHaveNulls()) {
        state.enableGroupIdTracking(seenGroupIds);
      }
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BooleanBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (values.isNull(groupPosition + positionOffset)) {
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BooleanVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(groupPosition + positionOffset));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BooleanBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (values.isNull(groupPosition + positionOffset)) {
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BooleanVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(groupPosition + positionOffset));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BooleanBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(v));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BooleanVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      CountDistinctBooleanAggregator.combine(state, groupId, values.getBoolean(groupPosition + positionOffset));
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block fbitUncast = page.getBlock(channels.get(0));
    if (fbitUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector fbit = ((BooleanBlock) fbitUncast).asVector();
    Block tbitUncast = page.getBlock(channels.get(1));
    if (tbitUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector tbit = ((BooleanBlock) tbitUncast).asVector();
    assert fbit.getPositionCount() == tbit.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      CountDistinctBooleanAggregator.combineIntermediate(state, groupId, fbit.getBoolean(groupPosition + positionOffset), tbit.getBoolean(groupPosition + positionOffset));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    CountDistinctBooleanAggregator.GroupingState inState = ((CountDistinctBooleanGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    CountDistinctBooleanAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      GroupingAggregatorEvaluationContext evaluatorContext) {
    blocks[offset] = CountDistinctBooleanAggregator.evaluateFinal(state, selected, evaluatorContext.driverContext());
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
