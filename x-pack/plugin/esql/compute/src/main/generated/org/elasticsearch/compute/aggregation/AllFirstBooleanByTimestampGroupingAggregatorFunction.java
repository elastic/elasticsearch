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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link AllFirstBooleanByTimestampAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class AllFirstBooleanByTimestampGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("observed", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestampsPresent", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.BOOLEAN)  );

  private final AllFirstBooleanByTimestampAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public AllFirstBooleanByTimestampGroupingAggregatorFunction(List<Integer> channels,
      AllFirstBooleanByTimestampAggregator.GroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static AllFirstBooleanByTimestampGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext) {
    return new AllFirstBooleanByTimestampGroupingAggregatorFunction(channels, AllFirstBooleanByTimestampAggregator.initGrouping(driverContext), driverContext);
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
    BooleanBlock valuesBlock = page.getBlock(channels.get(0));
    LongBlock timestampsBlock = page.getBlock(channels.get(1));
    maybeEnableGroupIdTracking(seenGroupIds, valuesBlock, timestampsBlock);
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock, timestampsBlock);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock, timestampsBlock);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock, timestampsBlock);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BooleanBlock valuesBlock,
      LongBlock timestampsBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        AllFirstBooleanByTimestampAggregator.combine(state, groupId, valuesPosition, valuesBlock, timestampsBlock);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block observedUncast = page.getBlock(channels.get(0));
    BooleanBlock observed = (BooleanBlock) observedUncast;
    Block timestampsPresentUncast = page.getBlock(channels.get(1));
    BooleanBlock timestampsPresent = (BooleanBlock) timestampsPresentUncast;
    Block timestampsUncast = page.getBlock(channels.get(2));
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(3));
    BooleanBlock values = (BooleanBlock) valuesUncast;
    assert observed.getPositionCount() == timestampsPresent.getPositionCount() && observed.getPositionCount() == timestamps.getPositionCount() && observed.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AllFirstBooleanByTimestampAggregator.combineIntermediate(state, groupId, observed, timestampsPresent, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BooleanBlock valuesBlock,
      LongBlock timestampsBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        AllFirstBooleanByTimestampAggregator.combine(state, groupId, valuesPosition, valuesBlock, timestampsBlock);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block observedUncast = page.getBlock(channels.get(0));
    BooleanBlock observed = (BooleanBlock) observedUncast;
    Block timestampsPresentUncast = page.getBlock(channels.get(1));
    BooleanBlock timestampsPresent = (BooleanBlock) timestampsPresentUncast;
    Block timestampsUncast = page.getBlock(channels.get(2));
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(3));
    BooleanBlock values = (BooleanBlock) valuesUncast;
    assert observed.getPositionCount() == timestampsPresent.getPositionCount() && observed.getPositionCount() == timestamps.getPositionCount() && observed.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        AllFirstBooleanByTimestampAggregator.combineIntermediate(state, groupId, observed, timestampsPresent, timestamps, values, valuesPosition);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BooleanBlock valuesBlock,
      LongBlock timestampsBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      AllFirstBooleanByTimestampAggregator.combine(state, groupId, valuesPosition, valuesBlock, timestampsBlock);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block observedUncast = page.getBlock(channels.get(0));
    BooleanBlock observed = (BooleanBlock) observedUncast;
    Block timestampsPresentUncast = page.getBlock(channels.get(1));
    BooleanBlock timestampsPresent = (BooleanBlock) timestampsPresentUncast;
    Block timestampsUncast = page.getBlock(channels.get(2));
    LongBlock timestamps = (LongBlock) timestampsUncast;
    Block valuesUncast = page.getBlock(channels.get(3));
    BooleanBlock values = (BooleanBlock) valuesUncast;
    assert observed.getPositionCount() == timestampsPresent.getPositionCount() && observed.getPositionCount() == timestamps.getPositionCount() && observed.getPositionCount() == values.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      AllFirstBooleanByTimestampAggregator.combineIntermediate(state, groupId, observed, timestampsPresent, timestamps, values, valuesPosition);
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BooleanBlock valuesBlock,
      LongBlock timestampsBlock) {
    if (valuesBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (timestampsBlock.mayHaveNulls()) {
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
    blocks[offset] = AllFirstBooleanByTimestampAggregator.evaluateFinal(state, selected, ctx);
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
