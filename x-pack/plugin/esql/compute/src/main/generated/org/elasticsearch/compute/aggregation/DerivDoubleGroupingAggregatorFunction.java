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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link DerivDoubleAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class DerivDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.DOUBLE)  );

  private final DerivDoubleAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public DerivDoubleGroupingAggregatorFunction(List<Integer> channels,
      DerivDoubleAggregator.GroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static DerivDoubleGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext) {
    return new DerivDoubleGroupingAggregatorFunction(channels, DerivDoubleAggregator.initGrouping(driverContext), driverContext);
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
    LongBlock timestampBlock = page.getBlock(channels.get(0));
    DoubleBlock valueBlock = page.getBlock(channels.get(1));
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, timestampBlock, valueBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    DoubleVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, timestampBlock, valueBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, valueBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, valueVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, valueVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, valueVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongBlock timestampBlock,
      DoubleBlock valueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
          int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
          for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
            double valueValue = valueBlock.getDouble(valueOffset);
            DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongVector timestampVector,
      DoubleVector valueVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        long timestampValue = timestampVector.getLong(valuesPosition);
        double valueValue = valueVector.getDouble(valuesPosition);
        DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
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
    DoubleBlock values = (DoubleBlock) valuesUncast;
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
        DerivDoubleAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongBlock timestampBlock,
      DoubleBlock valueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
          int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
          for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
            double valueValue = valueBlock.getDouble(valueOffset);
            DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongVector timestampVector,
      DoubleVector valueVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        long timestampValue = timestampVector.getLong(valuesPosition);
        double valueValue = valueVector.getDouble(valuesPosition);
        DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
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
    DoubleBlock values = (DoubleBlock) valuesUncast;
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
        DerivDoubleAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock timestampBlock,
      DoubleBlock valueBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
      int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
      for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
        long timestampValue = timestampBlock.getLong(timestampOffset);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          double valueValue = valueBlock.getDouble(valueOffset);
          DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector timestampVector,
      DoubleVector valueVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      double valueValue = valueVector.getDouble(valuesPosition);
      DerivDoubleAggregator.combine(state, groupId, timestampValue, valueValue);
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
    DoubleBlock values = (DoubleBlock) valuesUncast;
    assert timestamps.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      DerivDoubleAggregator.combineIntermediate(state, groupId, timestamps, values, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, LongBlock timestampBlock,
      DoubleBlock valueBlock) {
    if (timestampBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (valueBlock.mayHaveNulls()) {
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
    blocks[offset] = DerivDoubleAggregator.evaluateFinal(state, selected, ctx);
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
