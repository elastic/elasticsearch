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
 * {@link GroupingAggregatorFunction} implementation for {@link TopDoubleLongAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class TopDoubleLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.DOUBLE),
      new IntermediateStateDesc("output", ElementType.LONG)  );

  private final TopDoubleLongAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final int limit;

  private final boolean ascending;

  TopDoubleLongGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext,
      int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
    this.channels = channels;
    this.state = TopDoubleLongAggregator.initGrouping(driverContext.bigArrays(), limit, ascending);
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
    DoubleBlock vBlock = page.getBlock(channels.get(0));
    LongBlock outputValueBlock = page.getBlock(channels.get(1));
    if (vBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    if (outputValueBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    DoubleVector vVector = vBlock.asVector();
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

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleBlock vBlock,
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
          double vValue = vBlock.getDouble(vOffset);
          int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
          int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
          for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
            long outputValueValue = outputValueBlock.getLong(outputValueOffset);
            TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleVector vVector,
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
        double vValue = vVector.getDouble(valuesPosition);
        long outputValueValue = outputValueVector.getLong(valuesPosition);
        TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
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
    DoubleBlock top = (DoubleBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
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
        TopDoubleLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleBlock vBlock,
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
          double vValue = vBlock.getDouble(vOffset);
          int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
          int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
          for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
            long outputValueValue = outputValueBlock.getLong(outputValueOffset);
            TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleVector vVector,
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
        double vValue = vVector.getDouble(valuesPosition);
        long outputValueValue = outputValueVector.getLong(valuesPosition);
        TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
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
    DoubleBlock top = (DoubleBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
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
        TopDoubleLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleBlock vBlock,
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
        double vValue = vBlock.getDouble(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(valuesPosition);
        int outputValueEnd = outputValueStart + outputValueBlock.getValueCount(valuesPosition);
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          long outputValueValue = outputValueBlock.getLong(outputValueOffset);
          TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleVector vVector,
      LongVector outputValueVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      double vValue = vVector.getDouble(valuesPosition);
      long outputValueValue = outputValueVector.getLong(valuesPosition);
      TopDoubleLongAggregator.combine(state, groupId, vValue, outputValueValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
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
    DoubleBlock top = (DoubleBlock) topUncast;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
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
    LongBlock output = (LongBlock) outputUncast;
    assert top.getPositionCount() == output.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      TopDoubleLongAggregator.combineIntermediate(state, groupId, top, output, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, DoubleBlock vBlock,
      LongBlock outputValueBlock) {
    if (vBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (outputValueBlock.mayHaveNulls()) {
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
    return this::evaluateIntermediate;
  }

  private void evaluateIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
    state.toIntermediate(blocks, offset, selectedInPage, driverContext);
  }

  @Override
  public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(IntVector selected,
      GroupingAggregatorEvaluationContext ctx) {
    return (blocks, offset, selectedInPage) -> evaluateFinal(blocks, offset, selectedInPage, ctx);
  }

  private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage,
      GroupingAggregatorEvaluationContext ctx) {
    blocks[offset] = TopDoubleLongAggregator.evaluateFinal(state, selectedInPage, ctx);
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
