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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SumFloatAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SumFloatGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.DOUBLE),
      new IntermediateStateDesc("delta", ElementType.DOUBLE),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final SumDoubleAggregator.GroupingSumState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SumFloatGroupingAggregatorFunction(List<Integer> channels,
      SumDoubleAggregator.GroupingSumState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SumFloatGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext) {
    return new SumFloatGroupingAggregatorFunction(channels, SumFloatAggregator.initGrouping(driverContext.bigArrays()), driverContext);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds,
      Page page) {
    FloatBlock vBlock = page.getBlock(channels.get(0));
    FloatVector vVector = vBlock.asVector();
    if (vVector == null) {
      if (vBlock.mayHaveNulls()) {
        state.enableGroupIdTracking(seenGroupIds);
      }
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, FloatBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          float vValue = vBlock.getFloat(vOffset);
          SumFloatAggregator.combine(state, groupId, vValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, FloatVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        float vValue = vVector.getFloat(valuesPosition);
        SumFloatAggregator.combine(state, groupId, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector value = ((DoubleBlock) valueUncast).asVector();
    Block deltaUncast = page.getBlock(channels.get(1));
    if (deltaUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert value.getPositionCount() == delta.getPositionCount() && value.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SumFloatAggregator.combineIntermediate(state, groupId, value.getDouble(valuesPosition), delta.getDouble(valuesPosition), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          float vValue = vBlock.getFloat(vOffset);
          SumFloatAggregator.combine(state, groupId, vValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        float vValue = vVector.getFloat(valuesPosition);
        SumFloatAggregator.combine(state, groupId, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector value = ((DoubleBlock) valueUncast).asVector();
    Block deltaUncast = page.getBlock(channels.get(1));
    if (deltaUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert value.getPositionCount() == delta.getPositionCount() && value.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SumFloatAggregator.combineIntermediate(state, groupId, value.getDouble(valuesPosition), delta.getDouble(valuesPosition), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, FloatBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int vStart = vBlock.getFirstValueIndex(valuesPosition);
      int vEnd = vStart + vBlock.getValueCount(valuesPosition);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        SumFloatAggregator.combine(state, groupId, vValue);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, FloatVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      float vValue = vVector.getFloat(valuesPosition);
      SumFloatAggregator.combine(state, groupId, vValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector value = ((DoubleBlock) valueUncast).asVector();
    Block deltaUncast = page.getBlock(channels.get(1));
    if (deltaUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert value.getPositionCount() == delta.getPositionCount() && value.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      SumFloatAggregator.combineIntermediate(state, groupId, value.getDouble(valuesPosition), delta.getDouble(valuesPosition), seen.getBoolean(valuesPosition));
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      GroupingAggregatorEvaluationContext ctx) {
    blocks[offset] = SumFloatAggregator.evaluateFinal(state, selected, ctx);
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
