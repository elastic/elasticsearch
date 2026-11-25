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
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link AllLastExponentialHistogramByIntAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class AllLastExponentialHistogramByIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sortKeys", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final ExponentialHistogramStates.WithLongGroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  AllLastExponentialHistogramByIntGroupingAggregatorFunction(List<Integer> channels,
      DriverContext driverContext) {
    this.channels = channels;
    this.state = AllLastExponentialHistogramByIntAggregator.initGrouping(driverContext);
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
    ExponentialHistogramBlock valueBlock = page.getBlock(channels.get(0));
    IntBlock sortKeyBlock = page.getBlock(channels.get(1));
    if (valueBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    if (sortKeyBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    IntVector sortKeyVector = sortKeyBlock.asVector();
    if (sortKeyVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, valueBlock, sortKeyBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, sortKeyBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, sortKeyBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valueBlock, sortKeyBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock, sortKeyVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock, sortKeyVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock, sortKeyVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups,
      ExponentialHistogramBlock valueBlock, IntBlock sortKeyBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (sortKeyBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          int sortKeyStart = sortKeyBlock.getFirstValueIndex(valuesPosition);
          int sortKeyEnd = sortKeyStart + sortKeyBlock.getValueCount(valuesPosition);
          for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
            int sortKeyValue = sortKeyBlock.getInt(sortKeyOffset);
            AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups,
      ExponentialHistogramBlock valueBlock, IntVector sortKeyVector) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int sortKeyValue = sortKeyVector.getInt(valuesPosition);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block sortKeysUncast = page.getBlock(channels.get(0));
    if (sortKeysUncast.areAllValuesNull()) {
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
    LongVector sortKeys = ((LongBlock) sortKeysUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
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
    ExponentialHistogramBlock values = (ExponentialHistogramBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
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
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert sortKeys.getPositionCount() == values.getPositionCount() && sortKeys.getPositionCount() == seen.getPositionCount();
    ExponentialHistogramScratch valuesScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AllLastExponentialHistogramByIntAggregator.combineIntermediate(state, groupId, sortKeys.getLong(valuesPosition), values, seen.getBoolean(valuesPosition), valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups,
      ExponentialHistogramBlock valueBlock, IntBlock sortKeyBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (sortKeyBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          int sortKeyStart = sortKeyBlock.getFirstValueIndex(valuesPosition);
          int sortKeyEnd = sortKeyStart + sortKeyBlock.getValueCount(valuesPosition);
          for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
            int sortKeyValue = sortKeyBlock.getInt(sortKeyOffset);
            AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups,
      ExponentialHistogramBlock valueBlock, IntVector sortKeyVector) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int sortKeyValue = sortKeyVector.getInt(valuesPosition);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block sortKeysUncast = page.getBlock(channels.get(0));
    if (sortKeysUncast.areAllValuesNull()) {
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
    LongVector sortKeys = ((LongBlock) sortKeysUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
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
    ExponentialHistogramBlock values = (ExponentialHistogramBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
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
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert sortKeys.getPositionCount() == values.getPositionCount() && sortKeys.getPositionCount() == seen.getPositionCount();
    ExponentialHistogramScratch valuesScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AllLastExponentialHistogramByIntAggregator.combineIntermediate(state, groupId, sortKeys.getLong(valuesPosition), values, seen.getBoolean(valuesPosition), valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups,
      ExponentialHistogramBlock valueBlock, IntBlock sortKeyBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      if (sortKeyBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
      int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        int sortKeyStart = sortKeyBlock.getFirstValueIndex(valuesPosition);
        int sortKeyEnd = sortKeyStart + sortKeyBlock.getValueCount(valuesPosition);
        for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
          int sortKeyValue = sortKeyBlock.getInt(sortKeyOffset);
          AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups,
      ExponentialHistogramBlock valueBlock, IntVector sortKeyVector) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      int sortKeyValue = sortKeyVector.getInt(valuesPosition);
      int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
      int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        AllLastExponentialHistogramByIntAggregator.combine(state, groupId, valueValue, sortKeyValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block sortKeysUncast = page.getBlock(channels.get(0));
    if (sortKeysUncast.areAllValuesNull()) {
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
    LongVector sortKeys = ((LongBlock) sortKeysUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
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
    ExponentialHistogramBlock values = (ExponentialHistogramBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
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
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert sortKeys.getPositionCount() == values.getPositionCount() && sortKeys.getPositionCount() == seen.getPositionCount();
    ExponentialHistogramScratch valuesScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      AllLastExponentialHistogramByIntAggregator.combineIntermediate(state, groupId, sortKeys.getLong(valuesPosition), values, seen.getBoolean(valuesPosition), valuesPosition);
    }
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessIntermediateInputPage(
      SeenGroupIds seenGroupIds, Page page) {
    BooleanVector seen = ((BooleanBlock) page.getBlock(channels.get(2))).asVector();
    if (seen == null || seen.isConstant() == false || seen.getBoolean(0) == false) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    return new GroupingAggregatorFunction.IntermediateAddInput(this, seenGroupIds, page);
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds,
      ExponentialHistogramBlock valueBlock, IntBlock sortKeyBlock) {
    if (valueBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (sortKeyBlock.mayHaveNulls()) {
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
    blocks[offset] = AllLastExponentialHistogramByIntAggregator.evaluateFinal(state, selectedInPage, ctx);
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
