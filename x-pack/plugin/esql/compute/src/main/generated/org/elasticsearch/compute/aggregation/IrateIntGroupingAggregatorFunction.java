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
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link IrateIntAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class IrateIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.INT)  );

  private final IrateIntAggregator.IntIrateGroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final boolean isDelta;

  private final boolean isDateNanos;

  public IrateIntGroupingAggregatorFunction(List<Integer> channels,
      IrateIntAggregator.IntIrateGroupingState state, DriverContext driverContext, boolean isDelta,
      boolean isDateNanos) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
    this.isDelta = isDelta;
    this.isDateNanos = isDateNanos;
  }

  public static IrateIntGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext, boolean isDelta, boolean isDateNanos) {
    return new IrateIntGroupingAggregatorFunction(channels, IrateIntAggregator.initGrouping(driverContext, isDelta, isDateNanos), driverContext, isDelta, isDateNanos);
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
    IntBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    IntVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, valueBlock, timestampBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, valueBlock, timestampBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, timestampBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueVector, timestampVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueVector, timestampVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valueVector, timestampVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, IntBlock valueBlock,
      LongBlock timestampBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          int valueValue = valueBlock.getInt(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, IntVector valueVector,
      LongVector timestampVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueValue = valueVector.getInt(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    IntBlock values = (IntBlock) valuesUncast;
    assert timestamps.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        IrateIntAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, IntBlock valueBlock,
      LongBlock timestampBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          int valueValue = valueBlock.getInt(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, IntVector valueVector,
      LongVector timestampVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueValue = valueVector.getInt(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    IntBlock values = (IntBlock) valuesUncast;
    assert timestamps.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        IrateIntAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, IntBlock valueBlock,
      LongBlock timestampBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
      int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        int valueValue = valueBlock.getInt(valueOffset);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, IntVector valueVector,
      LongVector timestampVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      int valueValue = valueVector.getInt(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      IrateIntAggregator.combine(state, groupId, valueValue, timestampValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    IntBlock values = (IntBlock) valuesUncast;
    assert timestamps.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      IrateIntAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, IntBlock valueBlock,
      LongBlock timestampBlock) {
    if (valueBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (timestampBlock.mayHaveNulls()) {
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
    blocks[offset] = IrateIntAggregator.evaluateFinal(state, selected, ctx);
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
