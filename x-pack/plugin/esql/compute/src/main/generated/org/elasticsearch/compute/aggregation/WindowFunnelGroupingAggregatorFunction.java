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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link WindowFunnelAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class WindowFunnelGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("masks", ElementType.LONG)  );

  private final WindowFunnelAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final long span;

  private final int steps;

  WindowFunnelGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext,
      long span, int steps) {
    this.span = span;
    this.steps = steps;
    this.channels = channels;
    this.state = WindowFunnelAggregator.initGrouping(driverContext, span, steps);
    this.driverContext = driverContext;
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
    LongBlock maskBlock = page.getBlock(channels.get(1));
    if (timestampBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    if (maskBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, timestampBlock, maskBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    LongVector maskVector = maskBlock.asVector();
    if (maskVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, timestampBlock, maskBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, timestampBlock, maskBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, maskVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, maskVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, timestampVector, maskVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongBlock timestampBlock,
      LongBlock maskBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (maskBlock.isNull(valuesPosition)) {
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
          int maskStart = maskBlock.getFirstValueIndex(valuesPosition);
          int maskEnd = maskStart + maskBlock.getValueCount(valuesPosition);
          for (int maskOffset = maskStart; maskOffset < maskEnd; maskOffset++) {
            long maskValue = maskBlock.getLong(maskOffset);
            WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongVector timestampVector,
      LongVector maskVector) {
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
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block masksUncast = page.getBlock(channels.get(1));
    if (masksUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock masks = (LongBlock) masksUncast;
    assert timestamps.getPositionCount() == masks.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        WindowFunnelAggregator.combineIntermediate(state, groupId, timestamps, masks, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongBlock timestampBlock,
      LongBlock maskBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (maskBlock.isNull(valuesPosition)) {
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
          int maskStart = maskBlock.getFirstValueIndex(valuesPosition);
          int maskEnd = maskStart + maskBlock.getValueCount(valuesPosition);
          for (int maskOffset = maskStart; maskOffset < maskEnd; maskOffset++) {
            long maskValue = maskBlock.getLong(maskOffset);
            WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongVector timestampVector,
      LongVector maskVector) {
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
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block masksUncast = page.getBlock(channels.get(1));
    if (masksUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock masks = (LongBlock) masksUncast;
    assert timestamps.getPositionCount() == masks.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        WindowFunnelAggregator.combineIntermediate(state, groupId, timestamps, masks, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock timestampBlock,
      LongBlock maskBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      if (maskBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
      int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
      for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
        long timestampValue = timestampBlock.getLong(timestampOffset);
        int maskStart = maskBlock.getFirstValueIndex(valuesPosition);
        int maskEnd = maskStart + maskBlock.getValueCount(valuesPosition);
        for (int maskOffset = maskStart; maskOffset < maskEnd; maskOffset++) {
          long maskValue = maskBlock.getLong(maskOffset);
          WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector timestampVector,
      LongVector maskVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      long maskValue = maskVector.getLong(valuesPosition);
      WindowFunnelAggregator.combine(state, groupId, timestampValue, maskValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block masksUncast = page.getBlock(channels.get(1));
    if (masksUncast.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block.
       * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
       *       being fast without this. Likely the branch predictor is kicking
       *       in there. But we do this anyway, just so we don't have to trust
       *       it. It's magic. Glorious magic. But it's deep magic. And we won't
       *       always have long sequences of ConstantNullBlock. And this code
       *       shows readers we've thought about this.
       */
      return;
    }
    LongBlock masks = (LongBlock) masksUncast;
    assert timestamps.getPositionCount() == masks.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      WindowFunnelAggregator.combineIntermediate(state, groupId, timestamps, masks, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, LongBlock timestampBlock,
      LongBlock maskBlock) {
    if (timestampBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (maskBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
      IntVector selected, GroupingAggregatorEvaluationContext ctx) {
    return WindowFunnelAggregator.prepareEvaluateIntermediate(state, selected, ctx);
  }

  @Override
  public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(IntVector selected,
      GroupingAggregatorEvaluationContext ctx) {
    return WindowFunnelAggregator.prepareEvaluateFinal(state, selected, ctx);
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
