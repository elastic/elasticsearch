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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link TopFloatLongAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class TopFloatLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.FLOAT),
      new IntermediateStateDesc("output", ElementType.LONG)  );

  private final TopFloatLongAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final int limit;

  private final boolean ascending;

  public TopFloatLongGroupingAggregatorFunction(List<Integer> channels,
      TopFloatLongAggregator.GroupingState state, DriverContext driverContext, int limit,
      boolean ascending) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopFloatLongGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext, int limit, boolean ascending) {
    return new TopFloatLongGroupingAggregatorFunction(channels, TopFloatLongAggregator.initGrouping(driverContext.bigArrays(), limit, ascending), driverContext, limit, ascending);
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
    LongBlock outputValueBlock = page.getBlock(channels.get(1));
    FloatVector vVector = vBlock.asVector();
    if (vVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, vBlock, outputValueBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    LongVector outputValueVector = outputValueBlock.asVector();
    if (outputValueVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, vBlock, outputValueBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, vBlock, outputValueBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector, outputValueVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector, outputValueVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, vVector, outputValueVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, FloatBlock vBlock,
      LongBlock outputValueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      if (outputValueBlock.isNull(valuesPosition)) {
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
          int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
          int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
          for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
            long outputValueValue = outputValueBlock.getLong(outputValueOffset);
            TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, FloatVector vVector,
      LongVector outputValueVector) {
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
        long outputValueValue = outputValueVector.getLong(valuesPosition);
        TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock top = (FloatBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
      return;
    }
    LongBlock output = (LongBlock) outputUncast;
    assert top.getPositionCount() == output.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        TopFloatLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatBlock vBlock,
      LongBlock outputValueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      if (outputValueBlock.isNull(valuesPosition)) {
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
          int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
          int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
          for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
            long outputValueValue = outputValueBlock.getLong(outputValueOffset);
            TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatVector vVector,
      LongVector outputValueVector) {
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
        long outputValueValue = outputValueVector.getLong(valuesPosition);
        TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock top = (FloatBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
      return;
    }
    LongBlock output = (LongBlock) outputUncast;
    assert top.getPositionCount() == output.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        TopFloatLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, FloatBlock vBlock,
      LongBlock outputValueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      if (outputValueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int vStart = vBlock.getFirstValueIndex(valuesPosition);
      int vEnd = vStart + vBlock.getValueCount(valuesPosition);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
        int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          long outputValueValue = outputValueBlock.getLong(outputValueOffset);
          TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, FloatVector vVector,
      LongVector outputValueVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      float vValue = vVector.getFloat(valuesPosition);
      long outputValueValue = outputValueVector.getLong(valuesPosition);
      TopFloatLongAggregator.combine(state, groupId, vValue, outputValueValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock top = (FloatBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
      return;
    }
    LongBlock output = (LongBlock) outputUncast;
    assert top.getPositionCount() == output.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      TopFloatLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, FloatBlock vBlock,
      LongBlock outputValueBlock) {
    if (vBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (outputValueBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
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
    blocks[offset] = TopFloatLongAggregator.evaluateFinal(state, selected, ctx);
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
