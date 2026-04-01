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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link FirstTDigestByTimestampAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class FirstTDigestByTimestampGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.TDIGEST),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final TDigestStates.WithLongGroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  FirstTDigestByTimestampGroupingAggregatorFunction(List<Integer> channels,
      DriverContext driverContext) {
    this.channels = channels;
    this.state = FirstTDigestByTimestampAggregator.initGrouping(driverContext);
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
    TDigestBlock tdigestBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    maybeEnableGroupIdTracking(seenGroupIds, tdigestBlock, timestampBlock);
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, tdigestBlock, timestampBlock);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, tdigestBlock, timestampBlock);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, tdigestBlock, timestampBlock);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, TDigestBlock tdigestBlock,
      LongBlock timestampBlock) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (tdigestBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int tdigestStart = tdigestBlock.getFirstValueIndex(valuesPosition);
        int tdigestEnd = tdigestStart + tdigestBlock.getValueCount(valuesPosition);
        for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
          TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            FirstTDigestByTimestampAggregator.combine(state, groupId, tdigestValue, timestampValue);
          }
        }
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
    LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert timestamps.getPositionCount() == values.getPositionCount() && timestamps.getPositionCount() == seen.getPositionCount();
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
        FirstTDigestByTimestampAggregator.combineIntermediate(state, groupId, timestamps.getLong(valuesPosition), values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, TDigestBlock tdigestBlock,
      LongBlock timestampBlock) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (tdigestBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int tdigestStart = tdigestBlock.getFirstValueIndex(valuesPosition);
        int tdigestEnd = tdigestStart + tdigestBlock.getValueCount(valuesPosition);
        for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
          TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
          int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
          int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
          for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
            long timestampValue = timestampBlock.getLong(timestampOffset);
            FirstTDigestByTimestampAggregator.combine(state, groupId, tdigestValue, timestampValue);
          }
        }
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
    LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert timestamps.getPositionCount() == values.getPositionCount() && timestamps.getPositionCount() == seen.getPositionCount();
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
        FirstTDigestByTimestampAggregator.combineIntermediate(state, groupId, timestamps.getLong(valuesPosition), values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, TDigestBlock tdigestBlock,
      LongBlock timestampBlock) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (tdigestBlock.isNull(valuesPosition)) {
        continue;
      }
      if (timestampBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int tdigestStart = tdigestBlock.getFirstValueIndex(valuesPosition);
      int tdigestEnd = tdigestStart + tdigestBlock.getValueCount(valuesPosition);
      for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
        TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
        int timestampStart = timestampBlock.getFirstValueIndex(valuesPosition);
        int timestampEnd = timestampStart + timestampBlock.getValueCount(valuesPosition);
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          FirstTDigestByTimestampAggregator.combine(state, groupId, tdigestValue, timestampValue);
        }
      }
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
    LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    TDigestBlock values = (TDigestBlock) valuesUncast;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert timestamps.getPositionCount() == values.getPositionCount() && timestamps.getPositionCount() == seen.getPositionCount();
    TDigestHolder valuesScratch = new TDigestHolder();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      FirstTDigestByTimestampAggregator.combineIntermediate(state, groupId, timestamps.getLong(valuesPosition), values.getTDigestHolder(values.getFirstValueIndex(valuesPosition), valuesScratch), seen.getBoolean(valuesPosition));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, TDigestBlock tdigestBlock,
      LongBlock timestampBlock) {
    if (tdigestBlock.mayHaveNulls()) {
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
    blocks[offset] = FirstTDigestByTimestampAggregator.evaluateFinal(state, selectedInPage, ctx);
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
