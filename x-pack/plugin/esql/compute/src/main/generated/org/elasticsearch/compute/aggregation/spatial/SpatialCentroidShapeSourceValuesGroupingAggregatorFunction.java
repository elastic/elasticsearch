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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialCentroidShapeSourceValuesAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SpatialCentroidShapeSourceValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("weight", ElementType.DOUBLE),
      new IntermediateStateDesc("shapeType", ElementType.INT)  );

  private final CentroidShapeAggregator.GroupingShapeCentroidState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SpatialCentroidShapeSourceValuesGroupingAggregatorFunction(List<Integer> channels,
      CentroidShapeAggregator.GroupingShapeCentroidState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SpatialCentroidShapeSourceValuesGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new SpatialCentroidShapeSourceValuesGroupingAggregatorFunction(channels, SpatialCentroidShapeSourceValuesAggregator.initGrouping(driverContext.bigArrays()), driverContext);
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
    BytesRefBlock wkbBlock = page.getBlock(channels.get(0));
    BytesRefVector wkbVector = wkbBlock.asVector();
    if (wkbVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, wkbBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, wkbBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, wkbBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, wkbBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, wkbVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, wkbVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, wkbVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefBlock wkbBlock) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (wkbBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int wkbStart = wkbBlock.getFirstValueIndex(valuesPosition);
        int wkbEnd = wkbStart + wkbBlock.getValueCount(valuesPosition);
        for (int wkbOffset = wkbStart; wkbOffset < wkbEnd; wkbOffset++) {
          BytesRef wkbValue = wkbBlock.getBytesRef(wkbOffset, wkbScratch);
          SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefVector wkbVector) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef wkbValue = wkbVector.getBytesRef(valuesPosition, wkbScratch);
        SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block xValUncast = page.getBlock(channels.get(0));
    if (xValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
    Block xDelUncast = page.getBlock(channels.get(1));
    if (xDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
    Block yValUncast = page.getBlock(channels.get(2));
    if (yValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
    Block yDelUncast = page.getBlock(channels.get(3));
    if (yDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
    Block weightUncast = page.getBlock(channels.get(4));
    if (weightUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector weight = ((DoubleBlock) weightUncast).asVector();
    Block shapeTypeUncast = page.getBlock(channels.get(5));
    if (shapeTypeUncast.areAllValuesNull()) {
      return;
    }
    IntVector shapeType = ((IntBlock) shapeTypeUncast).asVector();
    assert xVal.getPositionCount() == xDel.getPositionCount() && xVal.getPositionCount() == yVal.getPositionCount() && xVal.getPositionCount() == yDel.getPositionCount() && xVal.getPositionCount() == weight.getPositionCount() && xVal.getPositionCount() == shapeType.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SpatialCentroidShapeSourceValuesAggregator.combineIntermediate(state, groupId, xVal.getDouble(valuesPosition), xDel.getDouble(valuesPosition), yVal.getDouble(valuesPosition), yDel.getDouble(valuesPosition), weight.getDouble(valuesPosition), shapeType.getInt(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BytesRefBlock wkbBlock) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (wkbBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int wkbStart = wkbBlock.getFirstValueIndex(valuesPosition);
        int wkbEnd = wkbStart + wkbBlock.getValueCount(valuesPosition);
        for (int wkbOffset = wkbStart; wkbOffset < wkbEnd; wkbOffset++) {
          BytesRef wkbValue = wkbBlock.getBytesRef(wkbOffset, wkbScratch);
          SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BytesRefVector wkbVector) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef wkbValue = wkbVector.getBytesRef(valuesPosition, wkbScratch);
        SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block xValUncast = page.getBlock(channels.get(0));
    if (xValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
    Block xDelUncast = page.getBlock(channels.get(1));
    if (xDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
    Block yValUncast = page.getBlock(channels.get(2));
    if (yValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
    Block yDelUncast = page.getBlock(channels.get(3));
    if (yDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
    Block weightUncast = page.getBlock(channels.get(4));
    if (weightUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector weight = ((DoubleBlock) weightUncast).asVector();
    Block shapeTypeUncast = page.getBlock(channels.get(5));
    if (shapeTypeUncast.areAllValuesNull()) {
      return;
    }
    IntVector shapeType = ((IntBlock) shapeTypeUncast).asVector();
    assert xVal.getPositionCount() == xDel.getPositionCount() && xVal.getPositionCount() == yVal.getPositionCount() && xVal.getPositionCount() == yDel.getPositionCount() && xVal.getPositionCount() == weight.getPositionCount() && xVal.getPositionCount() == shapeType.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesPosition = groupPosition + positionOffset;
        SpatialCentroidShapeSourceValuesAggregator.combineIntermediate(state, groupId, xVal.getDouble(valuesPosition), xDel.getDouble(valuesPosition), yVal.getDouble(valuesPosition), yDel.getDouble(valuesPosition), weight.getDouble(valuesPosition), shapeType.getInt(valuesPosition));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefBlock wkbBlock) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (wkbBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int wkbStart = wkbBlock.getFirstValueIndex(valuesPosition);
      int wkbEnd = wkbStart + wkbBlock.getValueCount(valuesPosition);
      for (int wkbOffset = wkbStart; wkbOffset < wkbEnd; wkbOffset++) {
        BytesRef wkbValue = wkbBlock.getBytesRef(wkbOffset, wkbScratch);
        SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefVector wkbVector) {
    BytesRef wkbScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      BytesRef wkbValue = wkbVector.getBytesRef(valuesPosition, wkbScratch);
      SpatialCentroidShapeSourceValuesAggregator.combine(state, groupId, wkbValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block xValUncast = page.getBlock(channels.get(0));
    if (xValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
    Block xDelUncast = page.getBlock(channels.get(1));
    if (xDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
    Block yValUncast = page.getBlock(channels.get(2));
    if (yValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
    Block yDelUncast = page.getBlock(channels.get(3));
    if (yDelUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
    Block weightUncast = page.getBlock(channels.get(4));
    if (weightUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector weight = ((DoubleBlock) weightUncast).asVector();
    Block shapeTypeUncast = page.getBlock(channels.get(5));
    if (shapeTypeUncast.areAllValuesNull()) {
      return;
    }
    IntVector shapeType = ((IntBlock) shapeTypeUncast).asVector();
    assert xVal.getPositionCount() == xDel.getPositionCount() && xVal.getPositionCount() == yVal.getPositionCount() && xVal.getPositionCount() == yDel.getPositionCount() && xVal.getPositionCount() == weight.getPositionCount() && xVal.getPositionCount() == shapeType.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      int valuesPosition = groupPosition + positionOffset;
      SpatialCentroidShapeSourceValuesAggregator.combineIntermediate(state, groupId, xVal.getDouble(valuesPosition), xDel.getDouble(valuesPosition), yVal.getDouble(valuesPosition), yDel.getDouble(valuesPosition), weight.getDouble(valuesPosition), shapeType.getInt(valuesPosition));
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BytesRefBlock wkbBlock) {
    if (wkbBlock.mayHaveNulls()) {
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
    blocks[offset] = SpatialCentroidShapeSourceValuesAggregator.evaluateFinal(state, selected, ctx);
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
