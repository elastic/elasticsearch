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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link PromqlHistogramQuantileAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class PromqlHistogramQuantileGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("buckets", ElementType.DOUBLE)  );

  private final PromqlHistogramQuantileStates.GroupingState state;

  private final Warnings warnings;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final double quantile;

  PromqlHistogramQuantileGroupingAggregatorFunction(Warnings warnings, List<Integer> channels,
      DriverContext driverContext, double quantile) {
    this.quantile = quantile;
    this.warnings = warnings;
    this.channels = channels;
    this.state = PromqlHistogramQuantileAggregator.initGrouping(driverContext, quantile, warnings);
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
    DoubleBlock countBlock = page.getBlock(channels.get(0));
    BytesRefBlock upperBoundBlock = page.getBlock(channels.get(1));
    if (countBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    if (upperBoundBlock.areAllValuesNull()) {
      /*
       * All values are null so we can skip processing this block. But we
       * still need to track that some groups may not have been seen
       * so that they are initialized to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
      return null;
    }
    DoubleVector countVector = countBlock.asVector();
    if (countVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, countBlock, upperBoundBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    BytesRefVector upperBoundVector = upperBoundBlock.asVector();
    if (upperBoundVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, countBlock, upperBoundBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, countBlock, upperBoundBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, countVector, upperBoundVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, countVector, upperBoundVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, countVector, upperBoundVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleBlock countBlock,
      BytesRefBlock upperBoundBlock) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (countBlock.isNull(valuesPosition)) {
        continue;
      }
      if (upperBoundBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int countStart = countBlock.getFirstValueIndex(valuesPosition);
        int countEnd = countStart + countBlock.getValueCount(valuesPosition);
        for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
          double countValue = countBlock.getDouble(countOffset);
          int upperBoundStart = upperBoundBlock.getFirstValueIndex(valuesPosition);
          int upperBoundEnd = upperBoundStart + upperBoundBlock.getValueCount(valuesPosition);
          for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
            BytesRef upperBoundValue = upperBoundBlock.getBytesRef(upperBoundOffset, upperBoundScratch);
            PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleVector countVector,
      BytesRefVector upperBoundVector) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        double countValue = countVector.getDouble(valuesPosition);
        BytesRef upperBoundValue = upperBoundVector.getBytesRef(valuesPosition, upperBoundScratch);
        PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block bucketsUncast = page.getBlock(channels.get(0));
    DoubleBlock buckets = (DoubleBlock) bucketsUncast;
    PromqlHistogramQuantileAggregator.combineIntermediate(state, positionOffset, groups, buckets);
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleBlock countBlock,
      BytesRefBlock upperBoundBlock) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (countBlock.isNull(valuesPosition)) {
        continue;
      }
      if (upperBoundBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int countStart = countBlock.getFirstValueIndex(valuesPosition);
        int countEnd = countStart + countBlock.getValueCount(valuesPosition);
        for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
          double countValue = countBlock.getDouble(countOffset);
          int upperBoundStart = upperBoundBlock.getFirstValueIndex(valuesPosition);
          int upperBoundEnd = upperBoundStart + upperBoundBlock.getValueCount(valuesPosition);
          for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
            BytesRef upperBoundValue = upperBoundBlock.getBytesRef(upperBoundOffset, upperBoundScratch);
            PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleVector countVector,
      BytesRefVector upperBoundVector) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        double countValue = countVector.getDouble(valuesPosition);
        BytesRef upperBoundValue = upperBoundVector.getBytesRef(valuesPosition, upperBoundScratch);
        PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block bucketsUncast = page.getBlock(channels.get(0));
    DoubleBlock buckets = (DoubleBlock) bucketsUncast;
    PromqlHistogramQuantileAggregator.combineIntermediate(state, positionOffset, groups, buckets);
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleBlock countBlock,
      BytesRefBlock upperBoundBlock) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (countBlock.isNull(valuesPosition)) {
        continue;
      }
      if (upperBoundBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int countStart = countBlock.getFirstValueIndex(valuesPosition);
      int countEnd = countStart + countBlock.getValueCount(valuesPosition);
      for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
        double countValue = countBlock.getDouble(countOffset);
        int upperBoundStart = upperBoundBlock.getFirstValueIndex(valuesPosition);
        int upperBoundEnd = upperBoundStart + upperBoundBlock.getValueCount(valuesPosition);
        for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
          BytesRef upperBoundValue = upperBoundBlock.getBytesRef(upperBoundOffset, upperBoundScratch);
          PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleVector countVector,
      BytesRefVector upperBoundVector) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      double countValue = countVector.getDouble(valuesPosition);
      BytesRef upperBoundValue = upperBoundVector.getBytesRef(valuesPosition, upperBoundScratch);
      PromqlHistogramQuantileAggregator.combine(state, groupId, countValue, upperBoundValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block bucketsUncast = page.getBlock(channels.get(0));
    DoubleBlock buckets = (DoubleBlock) bucketsUncast;
    PromqlHistogramQuantileAggregator.combineIntermediate(state, positionOffset, groups, buckets);
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, DoubleBlock countBlock,
      BytesRefBlock upperBoundBlock) {
    if (countBlock.mayHaveNulls()) {
      /*
       * Some values in the block are null so some group ids may not
       * be seen. We need to track which ones so we can initialize
       * them to null when we read their values.
       */
      state.enableGroupIdTracking(seenGroupIds);
    }
    if (upperBoundBlock.mayHaveNulls()) {
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
    blocks[offset] = PromqlHistogramQuantileAggregator.evaluateFinal(state, selectedInPage, ctx);
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
