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
 * {@link GroupingAggregatorFunction} implementation for {@link DerivLongAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class DerivLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("count", ElementType.LONG),
      new IntermediateStateDesc("sumVal", ElementType.DOUBLE),
      new IntermediateStateDesc("sumTs", ElementType.LONG),
      new IntermediateStateDesc("sumTsVal", ElementType.DOUBLE),
      new IntermediateStateDesc("sumTsSq", ElementType.LONG)  );

  private final DerivDoubleAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public DerivLongGroupingAggregatorFunction(List<Integer> channels,
      DerivDoubleAggregator.GroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static DerivLongGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext) {
    return new DerivLongGroupingAggregatorFunction(channels, DerivLongAggregator.initGrouping(driverContext), driverContext);
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
    LongBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    LongVector valueVector = valueBlock.asVector();
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

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongBlock valueBlock,
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
          long valueValue = valueBlock.getLong(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongVector valueVector,
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
        long valueValue = valueVector.getLong(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    Block sumValUncast = page.getBlock(channels.get(1));
    if (sumValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumVal = ((DoubleBlock) sumValUncast).asVector();
    Block sumTsUncast = page.getBlock(channels.get(2));
    if (sumTsUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTs = ((LongBlock) sumTsUncast).asVector();
    Block sumTsValUncast = page.getBlock(channels.get(3));
    if (sumTsValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumTsVal = ((DoubleBlock) sumTsValUncast).asVector();
    Block sumTsSqUncast = page.getBlock(channels.get(4));
    if (sumTsSqUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTsSq = ((LongBlock) sumTsSqUncast).asVector();
    assert count.getPositionCount() == sumVal.getPositionCount() && count.getPositionCount() == sumTs.getPositionCount() && count.getPositionCount() == sumTsVal.getPositionCount() && count.getPositionCount() == sumTsSq.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        DerivLongAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), sumVal.getDouble(valuesPosition), sumTs.getLong(valuesPosition), sumTsVal.getDouble(valuesPosition), sumTsSq.getLong(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongBlock valueBlock,
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
          long valueValue = valueBlock.getLong(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongVector valueVector,
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
        long valueValue = valueVector.getLong(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    Block sumValUncast = page.getBlock(channels.get(1));
    if (sumValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumVal = ((DoubleBlock) sumValUncast).asVector();
    Block sumTsUncast = page.getBlock(channels.get(2));
    if (sumTsUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTs = ((LongBlock) sumTsUncast).asVector();
    Block sumTsValUncast = page.getBlock(channels.get(3));
    if (sumTsValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumTsVal = ((DoubleBlock) sumTsValUncast).asVector();
    Block sumTsSqUncast = page.getBlock(channels.get(4));
    if (sumTsSqUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTsSq = ((LongBlock) sumTsSqUncast).asVector();
    assert count.getPositionCount() == sumVal.getPositionCount() && count.getPositionCount() == sumTs.getPositionCount() && count.getPositionCount() == sumTsVal.getPositionCount() && count.getPositionCount() == sumTsSq.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        DerivLongAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), sumVal.getDouble(valuesPosition), sumTs.getLong(valuesPosition), sumTsVal.getDouble(valuesPosition), sumTsSq.getLong(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock valueBlock,
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
        long valueValue = valueBlock.getLong(valueOffset);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector valueVector,
      LongVector timestampVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      DerivLongAggregator.combine(state, groupId, valueValue, timestampValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    Block sumValUncast = page.getBlock(channels.get(1));
    if (sumValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumVal = ((DoubleBlock) sumValUncast).asVector();
    Block sumTsUncast = page.getBlock(channels.get(2));
    if (sumTsUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTs = ((LongBlock) sumTsUncast).asVector();
    Block sumTsValUncast = page.getBlock(channels.get(3));
    if (sumTsValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumTsVal = ((DoubleBlock) sumTsValUncast).asVector();
    Block sumTsSqUncast = page.getBlock(channels.get(4));
    if (sumTsSqUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTsSq = ((LongBlock) sumTsSqUncast).asVector();
    assert count.getPositionCount() == sumVal.getPositionCount() && count.getPositionCount() == sumTs.getPositionCount() && count.getPositionCount() == sumTsVal.getPositionCount() && count.getPositionCount() == sumTsSq.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      DerivLongAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), sumVal.getDouble(valuesPosition), sumTs.getLong(valuesPosition), sumTsVal.getDouble(valuesPosition), sumTsSq.getLong(valuesPosition));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, LongBlock valueBlock,
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
    blocks[offset] = DerivLongAggregator.evaluateFinal(state, selected, ctx);
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
