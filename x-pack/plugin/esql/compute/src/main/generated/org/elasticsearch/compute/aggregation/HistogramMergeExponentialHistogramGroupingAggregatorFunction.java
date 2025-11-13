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
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link HistogramMergeExponentialHistogramAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class HistogramMergeExponentialHistogramGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.EXPONENTIAL_HISTOGRAM)  );

  private final ExponentialHistogramStates.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public HistogramMergeExponentialHistogramGroupingAggregatorFunction(List<Integer> channels,
      ExponentialHistogramStates.GroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static HistogramMergeExponentialHistogramGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new HistogramMergeExponentialHistogramGroupingAggregatorFunction(channels, HistogramMergeExponentialHistogramAggregator.initGrouping(driverContext.bigArrays(), driverContext), driverContext);
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

  private void addRawInput(int positionOffset, IntArrayBlock groups,
      ExponentialHistogramBlock valueBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
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
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          HistogramMergeExponentialHistogramAggregator.combine(state, groupId, valueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    ExponentialHistogramBlock value = (ExponentialHistogramBlock) valueUncast;
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        HistogramMergeExponentialHistogramAggregator.combineIntermediate(state, groupId, value.getExponentialHistogram(value.getFirstValueIndex(valuesPosition), valueScratch));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups,
      ExponentialHistogramBlock valueBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
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
          ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
          HistogramMergeExponentialHistogramAggregator.combine(state, groupId, valueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    ExponentialHistogramBlock value = (ExponentialHistogramBlock) valueUncast;
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        HistogramMergeExponentialHistogramAggregator.combineIntermediate(state, groupId, value.getExponentialHistogram(value.getFirstValueIndex(valuesPosition), valueScratch));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups,
      ExponentialHistogramBlock valueBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (valueBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int valueStart = valueBlock.getFirstValueIndex(valuesPosition);
      int valueEnd = valueStart + valueBlock.getValueCount(valuesPosition);
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        HistogramMergeExponentialHistogramAggregator.combine(state, groupId, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    ExponentialHistogramBlock value = (ExponentialHistogramBlock) valueUncast;
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      HistogramMergeExponentialHistogramAggregator.combineIntermediate(state, groupId, value.getExponentialHistogram(value.getFirstValueIndex(valuesPosition), valueScratch));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds,
      ExponentialHistogramBlock valueBlock) {
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
    blocks[offset] = HistogramMergeExponentialHistogramAggregator.evaluateFinal(state, selected, ctx);
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
