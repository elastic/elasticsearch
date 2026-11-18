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
 * {@link GroupingAggregatorFunction} implementation for {@link PredictLinearDoubleAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class PredictLinearDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("count", ElementType.LONG),
      new IntermediateStateDesc("origin", ElementType.LONG),
      new IntermediateStateDesc("x", ElementType.DOUBLE),
      new IntermediateStateDesc("cx", ElementType.DOUBLE),
      new IntermediateStateDesc("y", ElementType.DOUBLE),
      new IntermediateStateDesc("cy", ElementType.DOUBLE),
      new IntermediateStateDesc("xx", ElementType.DOUBLE),
      new IntermediateStateDesc("cxx", ElementType.DOUBLE),
      new IntermediateStateDesc("xy", ElementType.DOUBLE),
      new IntermediateStateDesc("cxy", ElementType.DOUBLE),
      new IntermediateStateDesc("constant", ElementType.DOUBLE)  );

  private final PredictLinearDoubleAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final double horizon;

  PredictLinearDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext,
      double horizon) {
    this.horizon = horizon;
    this.channels = channels;
    this.state = PredictLinearDoubleAggregator.initGrouping(driverContext, horizon);
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
    DoubleBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    if (valueBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    if (timestampBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    DoubleVector valueVector = valueBlock.asVector();
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

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleBlock valueBlock,
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
          double valueValue = valueBlock.getDouble(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleVector valueVector,
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
        double valueValue = valueVector.getDouble(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
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
    LongVector count = ((LongBlock) countUncast).asVector();
    Block originUncast = page.getBlock(channels.get(1));
    if (originUncast.areAllValuesNull()) {
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
    LongVector origin = ((LongBlock) originUncast).asVector();
    Block xUncast = page.getBlock(channels.get(2));
    if (xUncast.areAllValuesNull()) {
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
    DoubleVector x = ((DoubleBlock) xUncast).asVector();
    Block cxUncast = page.getBlock(channels.get(3));
    if (cxUncast.areAllValuesNull()) {
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
    DoubleVector cx = ((DoubleBlock) cxUncast).asVector();
    Block yUncast = page.getBlock(channels.get(4));
    if (yUncast.areAllValuesNull()) {
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
    DoubleVector y = ((DoubleBlock) yUncast).asVector();
    Block cyUncast = page.getBlock(channels.get(5));
    if (cyUncast.areAllValuesNull()) {
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
    DoubleVector cy = ((DoubleBlock) cyUncast).asVector();
    Block xxUncast = page.getBlock(channels.get(6));
    if (xxUncast.areAllValuesNull()) {
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
    DoubleVector xx = ((DoubleBlock) xxUncast).asVector();
    Block cxxUncast = page.getBlock(channels.get(7));
    if (cxxUncast.areAllValuesNull()) {
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
    DoubleVector cxx = ((DoubleBlock) cxxUncast).asVector();
    Block xyUncast = page.getBlock(channels.get(8));
    if (xyUncast.areAllValuesNull()) {
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
    DoubleVector xy = ((DoubleBlock) xyUncast).asVector();
    Block cxyUncast = page.getBlock(channels.get(9));
    if (cxyUncast.areAllValuesNull()) {
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
    DoubleVector cxy = ((DoubleBlock) cxyUncast).asVector();
    Block constantUncast = page.getBlock(channels.get(10));
    if (constantUncast.areAllValuesNull()) {
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
    DoubleVector constant = ((DoubleBlock) constantUncast).asVector();
    assert count.getPositionCount() == origin.getPositionCount() && count.getPositionCount() == x.getPositionCount() && count.getPositionCount() == cx.getPositionCount() && count.getPositionCount() == y.getPositionCount() && count.getPositionCount() == cy.getPositionCount() && count.getPositionCount() == xx.getPositionCount() && count.getPositionCount() == cxx.getPositionCount() && count.getPositionCount() == xy.getPositionCount() && count.getPositionCount() == cxy.getPositionCount() && count.getPositionCount() == constant.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        PredictLinearDoubleAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), origin.getLong(valuesPosition), x.getDouble(valuesPosition), cx.getDouble(valuesPosition), y.getDouble(valuesPosition), cy.getDouble(valuesPosition), xx.getDouble(valuesPosition), cxx.getDouble(valuesPosition), xy.getDouble(valuesPosition), cxy.getDouble(valuesPosition), constant.getDouble(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleBlock valueBlock,
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
          double valueValue = valueBlock.getDouble(valueOffset);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleVector valueVector,
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
        double valueValue = valueVector.getDouble(valuesPosition);
        long timestampValue = timestampVector.getLong(valuesPosition);
        PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
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
    LongVector count = ((LongBlock) countUncast).asVector();
    Block originUncast = page.getBlock(channels.get(1));
    if (originUncast.areAllValuesNull()) {
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
    LongVector origin = ((LongBlock) originUncast).asVector();
    Block xUncast = page.getBlock(channels.get(2));
    if (xUncast.areAllValuesNull()) {
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
    DoubleVector x = ((DoubleBlock) xUncast).asVector();
    Block cxUncast = page.getBlock(channels.get(3));
    if (cxUncast.areAllValuesNull()) {
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
    DoubleVector cx = ((DoubleBlock) cxUncast).asVector();
    Block yUncast = page.getBlock(channels.get(4));
    if (yUncast.areAllValuesNull()) {
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
    DoubleVector y = ((DoubleBlock) yUncast).asVector();
    Block cyUncast = page.getBlock(channels.get(5));
    if (cyUncast.areAllValuesNull()) {
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
    DoubleVector cy = ((DoubleBlock) cyUncast).asVector();
    Block xxUncast = page.getBlock(channels.get(6));
    if (xxUncast.areAllValuesNull()) {
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
    DoubleVector xx = ((DoubleBlock) xxUncast).asVector();
    Block cxxUncast = page.getBlock(channels.get(7));
    if (cxxUncast.areAllValuesNull()) {
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
    DoubleVector cxx = ((DoubleBlock) cxxUncast).asVector();
    Block xyUncast = page.getBlock(channels.get(8));
    if (xyUncast.areAllValuesNull()) {
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
    DoubleVector xy = ((DoubleBlock) xyUncast).asVector();
    Block cxyUncast = page.getBlock(channels.get(9));
    if (cxyUncast.areAllValuesNull()) {
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
    DoubleVector cxy = ((DoubleBlock) cxyUncast).asVector();
    Block constantUncast = page.getBlock(channels.get(10));
    if (constantUncast.areAllValuesNull()) {
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
    DoubleVector constant = ((DoubleBlock) constantUncast).asVector();
    assert count.getPositionCount() == origin.getPositionCount() && count.getPositionCount() == x.getPositionCount() && count.getPositionCount() == cx.getPositionCount() && count.getPositionCount() == y.getPositionCount() && count.getPositionCount() == cy.getPositionCount() && count.getPositionCount() == xx.getPositionCount() && count.getPositionCount() == cxx.getPositionCount() && count.getPositionCount() == xy.getPositionCount() && count.getPositionCount() == cxy.getPositionCount() && count.getPositionCount() == constant.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        PredictLinearDoubleAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), origin.getLong(valuesPosition), x.getDouble(valuesPosition), cx.getDouble(valuesPosition), y.getDouble(valuesPosition), cy.getDouble(valuesPosition), xx.getDouble(valuesPosition), cxx.getDouble(valuesPosition), xy.getDouble(valuesPosition), cxy.getDouble(valuesPosition), constant.getDouble(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleBlock valueBlock,
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
        double valueValue = valueBlock.getDouble(valueOffset);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleVector valueVector,
      LongVector timestampVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      double valueValue = valueVector.getDouble(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      PredictLinearDoubleAggregator.combine(state, groupId, valueValue, timestampValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
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
    LongVector count = ((LongBlock) countUncast).asVector();
    Block originUncast = page.getBlock(channels.get(1));
    if (originUncast.areAllValuesNull()) {
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
    LongVector origin = ((LongBlock) originUncast).asVector();
    Block xUncast = page.getBlock(channels.get(2));
    if (xUncast.areAllValuesNull()) {
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
    DoubleVector x = ((DoubleBlock) xUncast).asVector();
    Block cxUncast = page.getBlock(channels.get(3));
    if (cxUncast.areAllValuesNull()) {
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
    DoubleVector cx = ((DoubleBlock) cxUncast).asVector();
    Block yUncast = page.getBlock(channels.get(4));
    if (yUncast.areAllValuesNull()) {
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
    DoubleVector y = ((DoubleBlock) yUncast).asVector();
    Block cyUncast = page.getBlock(channels.get(5));
    if (cyUncast.areAllValuesNull()) {
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
    DoubleVector cy = ((DoubleBlock) cyUncast).asVector();
    Block xxUncast = page.getBlock(channels.get(6));
    if (xxUncast.areAllValuesNull()) {
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
    DoubleVector xx = ((DoubleBlock) xxUncast).asVector();
    Block cxxUncast = page.getBlock(channels.get(7));
    if (cxxUncast.areAllValuesNull()) {
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
    DoubleVector cxx = ((DoubleBlock) cxxUncast).asVector();
    Block xyUncast = page.getBlock(channels.get(8));
    if (xyUncast.areAllValuesNull()) {
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
    DoubleVector xy = ((DoubleBlock) xyUncast).asVector();
    Block cxyUncast = page.getBlock(channels.get(9));
    if (cxyUncast.areAllValuesNull()) {
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
    DoubleVector cxy = ((DoubleBlock) cxyUncast).asVector();
    Block constantUncast = page.getBlock(channels.get(10));
    if (constantUncast.areAllValuesNull()) {
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
    DoubleVector constant = ((DoubleBlock) constantUncast).asVector();
    assert count.getPositionCount() == origin.getPositionCount() && count.getPositionCount() == x.getPositionCount() && count.getPositionCount() == cx.getPositionCount() && count.getPositionCount() == y.getPositionCount() && count.getPositionCount() == cy.getPositionCount() && count.getPositionCount() == xx.getPositionCount() && count.getPositionCount() == cxx.getPositionCount() && count.getPositionCount() == xy.getPositionCount() && count.getPositionCount() == cxy.getPositionCount() && count.getPositionCount() == constant.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      PredictLinearDoubleAggregator.combineIntermediate(state, groupId, count.getLong(valuesPosition), origin.getLong(valuesPosition), x.getDouble(valuesPosition), cx.getDouble(valuesPosition), y.getDouble(valuesPosition), cy.getDouble(valuesPosition), xx.getDouble(valuesPosition), cxx.getDouble(valuesPosition), xy.getDouble(valuesPosition), cxy.getDouble(valuesPosition), constant.getDouble(valuesPosition));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, DoubleBlock valueBlock,
      LongBlock timestampBlock) {
    if (valueBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (timestampBlock.mayHaveNulls()) {
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
    blocks[offset] = PredictLinearDoubleAggregator.evaluateFinal(state, selectedInPage, ctx);
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
