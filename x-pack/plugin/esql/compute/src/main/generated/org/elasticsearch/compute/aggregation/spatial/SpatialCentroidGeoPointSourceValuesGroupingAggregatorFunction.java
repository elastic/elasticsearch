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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialCentroidGeoPointSourceValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialCentroidGeoPointSourceValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("count", ElementType.LONG)  );

  private final CentroidPointAggregator.GroupingCentroidState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SpatialCentroidGeoPointSourceValuesGroupingAggregatorFunction(List<Integer> channels,
      CentroidPointAggregator.GroupingCentroidState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SpatialCentroidGeoPointSourceValuesGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new SpatialCentroidGeoPointSourceValuesGroupingAggregatorFunction(channels, SpatialCentroidGeoPointSourceValuesAggregator.initGrouping(driverContext.bigArrays()), driverContext);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds,
      Page page) {
    BytesRefBlock valuesBlock = page.getBlock(channels.get(0));
    BytesRefVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      if (valuesBlock.mayHaveNulls()) {
        state.enableGroupIdTracking(seenGroupIds);
      }
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }
    };
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefBlock values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        SpatialCentroidGeoPointSourceValuesAggregator.combine(state, groupId, values.getBytesRef(v, scratch));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefVector values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      SpatialCentroidGeoPointSourceValuesAggregator.combine(state, groupId, values.getBytesRef(groupPosition + positionOffset, scratch));
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, BytesRefBlock values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (values.isNull(groupPosition + positionOffset)) {
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          SpatialCentroidGeoPointSourceValuesAggregator.combine(state, groupId, values.getBytesRef(v, scratch));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, BytesRefVector values) {
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        SpatialCentroidGeoPointSourceValuesAggregator.combine(state, groupId, values.getBytesRef(groupPosition + positionOffset, scratch));
      }
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
    Block countUncast = page.getBlock(channels.get(4));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    assert xVal.getPositionCount() == xDel.getPositionCount() && xVal.getPositionCount() == yVal.getPositionCount() && xVal.getPositionCount() == yDel.getPositionCount() && xVal.getPositionCount() == count.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      SpatialCentroidGeoPointSourceValuesAggregator.combineIntermediate(state, groupId, xVal.getDouble(groupPosition + positionOffset), xDel.getDouble(groupPosition + positionOffset), yVal.getDouble(groupPosition + positionOffset), yDel.getDouble(groupPosition + positionOffset), count.getLong(groupPosition + positionOffset));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    CentroidPointAggregator.GroupingCentroidState inState = ((SpatialCentroidGeoPointSourceValuesGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    SpatialCentroidGeoPointSourceValuesAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      DriverContext driverContext) {
    blocks[offset] = SpatialCentroidGeoPointSourceValuesAggregator.evaluateFinal(state, selected, driverContext);
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
