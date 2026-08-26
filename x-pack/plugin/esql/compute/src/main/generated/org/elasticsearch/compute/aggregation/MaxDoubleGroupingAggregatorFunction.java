// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Double;
import java.lang.Integer;
import java.lang.Math;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MaxDoubleAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class MaxDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("max", ElementType.DOUBLE),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DoubleArrayState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  MaxDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
    this.channels = channels;
    this.state = new DoubleArrayState(driverContext.bigArrays(), MaxDoubleAggregator.init());
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
    if (vBlock.areAllValuesNull()) {
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

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleBlock vBlock) {
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
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          double vValue = vBlock.getDouble(vOffset);
          state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleVector vVector) {
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
        state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block maxUncast = page.getBlock(channels.get(0));
    if (maxUncast.areAllValuesNull()) {
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
    DoubleVector max = ((DoubleBlock) maxUncast).asVector();
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
    assert max.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        if (seen.getBoolean(valuesPosition)) {
          state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), max.getDouble(valuesPosition)));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleBlock vBlock) {
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
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          double vValue = vBlock.getDouble(vOffset);
          state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleVector vVector) {
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
        state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block maxUncast = page.getBlock(channels.get(0));
    if (maxUncast.areAllValuesNull()) {
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
    DoubleVector max = ((DoubleBlock) maxUncast).asVector();
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
    assert max.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        if (seen.getBoolean(valuesPosition)) {
          state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), max.getDouble(valuesPosition)));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int vStart = vBlock.getFirstValueIndex(valuesPosition);
      int vEnd = vStart + vBlock.getValueCount(valuesPosition);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        double vValue = vBlock.getDouble(vOffset);
        state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      double vValue = vVector.getDouble(valuesPosition);
      state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), vValue));
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    assert channels.size() == intermediateBlockCount();
    Block maxUncast = page.getBlock(channels.get(0));
    if (maxUncast.areAllValuesNull()) {
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
    DoubleVector max = ((DoubleBlock) maxUncast).asVector();
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
    assert max.getPositionCount() == seen.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      if (seen.getBoolean(valuesPosition)) {
        state.set(groupId, MaxDoubleAggregator.combine(state.getOrDefault(groupId), max.getDouble(valuesPosition)));
      }
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

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, DoubleBlock vBlock) {
    if (vBlock.mayHaveNulls()) {
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
  public boolean supportsPartitionedSplit() {
    return true;
  }

  @Override
  public void clear() {
    state.clear();
  }

  @Override
  public PartitionedHashTable.AggSplitter newSplitter() {
    return new MaxDoubleAggSplitter();
  }

  @Override
  public void combinePartition(PartitionedHashTable.PartitionedAgg source, int partition,
      int[] dstIds, int offset, int length) {
    final double[] sourceSums = ((MaxDoublePartitionedAgg) source).subs[partition];
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      state.set(dstIds[i], MaxDoubleAggregator.combine(state.getOrDefault(dstIds[i]), sourceSums[i]));
    }
  }

  @Override
  public void close() {
    state.close();
  }

  private final class MaxDoubleAggSplitter implements PartitionedHashTable.AggSplitter {
    private double[][] subs;

    private int[] lengths;

    MaxDoubleAggSplitter() {
    }

    @Override
    public void preAllocate(int[] partitionCounts) {
      subs = new double[partitionCounts.length][];
      lengths = new int[partitionCounts.length];
      for (int p = 0; p < partitionCounts.length; p++) {
        subs[p] = new double[partitionCounts[p]];
      }
    }

    @Override
    public void split(PartitionedHashTable.ScratchBuffer scratch, int idOffset, int totalPositions,
        short[] positions, int[] fills) {
      DoubleArray values = state.rawDoubleValues();
      double[] buffer = scratch.doubles;
      if (buffer.length < totalPositions) {
        buffer = scratch.doubles = new double[ArrayUtil.oversize(totalPositions, Double.BYTES)];
      }
      final int readLen = (int) Math.min(totalPositions, Math.max(0L, values.size() - idOffset));
      if (readLen > 0) {
        values.bulkGet(idOffset, buffer, 0, readLen);
      }
      if (readLen < totalPositions) {
        Arrays.fill(buffer, readLen, totalPositions, 0.0);
      }
      for (int p = 0; p < subs.length; p++) {
        final int c = fills[p];
        if (c == 0) {
          continue;
        }
        final int base = p * scratch.splitWriteBatchSize;
        double[] sub = subs[p];
        int dst = lengths[p];
        for (int i = 0; i < c; i++) {
          sub[dst++] = buffer[positions[base + i] & 0xFFFF];
        }
        lengths[p] += c;
      }
    }

    @Override
    public PartitionedHashTable.PartitionedAgg finish() {
      return new MaxDoublePartitionedAgg(subs);
    }

    @Override
    public void close() {
    }
  }

  static final class MaxDoublePartitionedAgg implements PartitionedHashTable.PartitionedAgg {
    final double[][] subs;

    MaxDoublePartitionedAgg(double[][] subs) {
      this.subs = subs;
    }

    @Override
    public void releasePartition(int partition) {
      subs[partition] = null;
    }

    @Override
    public void close() {
      Arrays.fill(subs, null);
    }
  }
}
