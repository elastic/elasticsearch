// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialExtentCartesianPointSourceValuesAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SpatialExtentCartesianPointSourceValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("minX", ElementType.INT),
      new IntermediateStateDesc("maxX", ElementType.INT),
      new IntermediateStateDesc("maxY", ElementType.INT),
      new IntermediateStateDesc("minY", ElementType.INT)  );

  private final SpatialExtentGroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SpatialExtentCartesianPointSourceValuesGroupingAggregatorFunction(List<Integer> channels,
      SpatialExtentGroupingState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SpatialExtentCartesianPointSourceValuesGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new SpatialExtentCartesianPointSourceValuesGroupingAggregatorFunction(channels, SpatialExtentCartesianPointSourceValuesAggregator.initGrouping(), driverContext);
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
    BytesRefBlock bytesBlock = page.getBlock(channels.get(0));
    BytesRefVector bytesVector = bytesBlock.asVector();
    if (bytesVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, bytesBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
        int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
        for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
          BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
          SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
        SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block minXUncast = page.getBlock(channels.get(0));
    if (minXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minX = ((IntBlock) minXUncast).asVector();
    Block maxXUncast = page.getBlock(channels.get(1));
    if (maxXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxX = ((IntBlock) maxXUncast).asVector();
    Block maxYUncast = page.getBlock(channels.get(2));
    if (maxYUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxY = ((IntBlock) maxYUncast).asVector();
    Block minYUncast = page.getBlock(channels.get(3));
    if (minYUncast.areAllValuesNull()) {
      return;
    }
    IntVector minY = ((IntBlock) minYUncast).asVector();
    assert minX.getPositionCount() == maxX.getPositionCount() && minX.getPositionCount() == maxY.getPositionCount() && minX.getPositionCount() == minY.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SpatialExtentCartesianPointSourceValuesAggregator.combineIntermediate(state, groupId, minX.getInt(valuesPosition), maxX.getInt(valuesPosition), maxY.getInt(valuesPosition), minY.getInt(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
        int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
        for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
          BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
          SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups,
      BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
        SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block minXUncast = page.getBlock(channels.get(0));
    if (minXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minX = ((IntBlock) minXUncast).asVector();
    Block maxXUncast = page.getBlock(channels.get(1));
    if (maxXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxX = ((IntBlock) maxXUncast).asVector();
    Block maxYUncast = page.getBlock(channels.get(2));
    if (maxYUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxY = ((IntBlock) maxYUncast).asVector();
    Block minYUncast = page.getBlock(channels.get(3));
    if (minYUncast.areAllValuesNull()) {
      return;
    }
    IntVector minY = ((IntBlock) minYUncast).asVector();
    assert minX.getPositionCount() == maxX.getPositionCount() && minX.getPositionCount() == maxY.getPositionCount() && minX.getPositionCount() == minY.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SpatialExtentCartesianPointSourceValuesAggregator.combineIntermediate(state, groupId, minX.getInt(valuesPosition), maxX.getInt(valuesPosition), maxY.getInt(valuesPosition), minY.getInt(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
      int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
      for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
        BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
        SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
      SpatialExtentCartesianPointSourceValuesAggregator.combine(state, groupId, bytesValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block minXUncast = page.getBlock(channels.get(0));
    if (minXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minX = ((IntBlock) minXUncast).asVector();
    Block maxXUncast = page.getBlock(channels.get(1));
    if (maxXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxX = ((IntBlock) maxXUncast).asVector();
    Block maxYUncast = page.getBlock(channels.get(2));
    if (maxYUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxY = ((IntBlock) maxYUncast).asVector();
    Block minYUncast = page.getBlock(channels.get(3));
    if (minYUncast.areAllValuesNull()) {
      return;
    }
    IntVector minY = ((IntBlock) minYUncast).asVector();
    assert minX.getPositionCount() == maxX.getPositionCount() && minX.getPositionCount() == maxY.getPositionCount() && minX.getPositionCount() == minY.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      SpatialExtentCartesianPointSourceValuesAggregator.combineIntermediate(state, groupId, minX.getInt(valuesPosition), maxX.getInt(valuesPosition), maxY.getInt(valuesPosition), minY.getInt(valuesPosition));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BytesRefBlock bytesBlock) {
    if (bytesBlock.mayHaveNulls()) {
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
    blocks[offset] = SpatialExtentCartesianPointSourceValuesAggregator.evaluateFinal(state, selected, ctx);
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
