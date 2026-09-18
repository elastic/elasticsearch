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
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link AnyTDigestAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class AnyTDigestGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("values", ElementType.TDIGEST),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final TDigestStates.SeenGroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  AnyTDigestGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
    this.channels = channels;
    this.state = AnyTDigestAggregator.initGrouping(driverContext);
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
    TDigestBlock valueBlock = page.getBlock(channels.get(0));
    if (valueBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    maybeEnableGroupIdTracking(seenGroupIds, valueBlock);
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valueBlock);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, TDigestBlock valueBlock) {
    TDigestHolder valueScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset, valueScratch);
          AnyTDigestAggregator.combine(state, groupId, valueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block valuesUncast = page.getBlock(channels.get(0));
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
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(1));
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
    assert values.getPositionCount() == seen.getPositionCount();
    TDigestHolder valuesScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AnyTDigestAggregator.combineIntermediate(state, groupId, values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, TDigestBlock valueBlock) {
    TDigestHolder valueScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
        int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
        for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
          TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset, valueScratch);
          AnyTDigestAggregator.combine(state, groupId, valueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block valuesUncast = page.getBlock(channels.get(0));
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
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(1));
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
    assert values.getPositionCount() == seen.getPositionCount();
    TDigestHolder valuesScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AnyTDigestAggregator.combineIntermediate(state, groupId, values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, TDigestBlock valueBlock) {
    TDigestHolder valueScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
      int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset, valueScratch);
        AnyTDigestAggregator.combine(state, groupId, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block valuesUncast = page.getBlock(channels.get(0));
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
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(1));
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
    assert values.getPositionCount() == seen.getPositionCount();
    TDigestHolder valuesScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      AnyTDigestAggregator.combineIntermediate(state, groupId, values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
    }
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessIntermediateInputPage(
      SeenGroupIds seenGroupIds, Page page) {
    BooleanVector seen = ((BooleanBlock) page.getBlock(channels.get(1))).asVector();
    if (seen == null || seen.isConstant() == false || seen.getBoolean(0) == false) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    return new GroupingAggregatorFunction.IntermediateAddInput(this, seenGroupIds, page);
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, TDigestBlock valueBlock) {
    if (valueBlock.mayHaveNulls()) {
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
    blocks[offset] = AnyTDigestAggregator.evaluateFinal(state, selectedInPage, ctx);
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
