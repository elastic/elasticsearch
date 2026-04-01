// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.ArithmeticException;
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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SumLongAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SumLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sum", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN),
      new IntermediateStateDesc("failed", ElementType.BOOLEAN)  );

  private final LongFallibleArrayState state;

  private final Warnings warnings;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  SumLongGroupingAggregatorFunction(Warnings warnings, List<Integer> channels,
      DriverContext driverContext) {
    this.warnings = warnings;
    this.channels = channels;
    this.state = new LongFallibleArrayState(driverContext.bigArrays(), SumLongAggregator.init());
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
    LongBlock vBlock = page.getBlock(channels.get(0));
    LongVector vVector = vBlock.asVector();
    if (vVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, vBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          long vValue = vBlock.getLong(vOffset);
          try {
            state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
          } catch (ArithmeticException e) {
            warnings.registerException(e);
            state.setFailed(groupId);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, LongVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        long vValue = vVector.getLong(valuesPosition);
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block sumUncast = page.getBlock(channels.get(0));
    if (sumUncast.areAllValuesNull()) {
      return;
    }
    LongVector sum = ((LongBlock) sumUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    Block failedUncast = page.getBlock(channels.get(2));
    if (failedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
    assert sum.getPositionCount() == seen.getPositionCount() && sum.getPositionCount() == failed.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        if (failed.getBoolean(valuesPosition)) {
          state.setFailed(groupId);
        } else if (seen.getBoolean(valuesPosition)) {
          try {
            state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), sum.getLong(valuesPosition)));
          } catch (ArithmeticException e) {
            warnings.registerException(e);
            state.setFailed(groupId);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          long vValue = vBlock.getLong(vOffset);
          try {
            state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
          } catch (ArithmeticException e) {
            warnings.registerException(e);
            state.setFailed(groupId);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, LongVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        long vValue = vVector.getLong(valuesPosition);
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block sumUncast = page.getBlock(channels.get(0));
    if (sumUncast.areAllValuesNull()) {
      return;
    }
    LongVector sum = ((LongBlock) sumUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    Block failedUncast = page.getBlock(channels.get(2));
    if (failedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
    assert sum.getPositionCount() == seen.getPositionCount() && sum.getPositionCount() == failed.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        if (failed.getBoolean(valuesPosition)) {
          state.setFailed(groupId);
        } else if (seen.getBoolean(valuesPosition)) {
          try {
            state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), sum.getLong(valuesPosition)));
          } catch (ArithmeticException e) {
            warnings.registerException(e);
            state.setFailed(groupId);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      if (state.hasFailed(groupId)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(valuesPosition);
      int vEnd = vStart + vBlock.getValueCount(valuesPosition);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        long vValue = vBlock.getLong(vOffset);
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      if (state.hasFailed(groupId)) {
        continue;
      }
      long vValue = vVector.getLong(valuesPosition);
      try {
        state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), vValue));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.setFailed(groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block sumUncast = page.getBlock(channels.get(0));
    if (sumUncast.areAllValuesNull()) {
      return;
    }
    LongVector sum = ((LongBlock) sumUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    Block failedUncast = page.getBlock(channels.get(2));
    if (failedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
    assert sum.getPositionCount() == seen.getPositionCount() && sum.getPositionCount() == failed.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      if (failed.getBoolean(valuesPosition)) {
        state.setFailed(groupId);
      } else if (seen.getBoolean(valuesPosition)) {
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), sum.getLong(valuesPosition)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, LongBlock vBlock) {
    if (vBlock.mayHaveNulls()) {
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
    blocks[offset] = state.toValuesBlock(selectedInPage, driverContext);
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
