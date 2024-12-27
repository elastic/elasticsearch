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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialExtentGeoPointDocValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialExtentGeoPointDocValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("minNegX", ElementType.INT),
      new IntermediateStateDesc("minPosX", ElementType.INT),
      new IntermediateStateDesc("maxNegX", ElementType.INT),
      new IntermediateStateDesc("maxPosX", ElementType.INT),
      new IntermediateStateDesc("maxY", ElementType.INT),
      new IntermediateStateDesc("minY", ElementType.INT)  );

  private final SpatialExtentGroupingStateWrappedLongitudeState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SpatialExtentGeoPointDocValuesGroupingAggregatorFunction(List<Integer> channels,
      SpatialExtentGroupingStateWrappedLongitudeState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SpatialExtentGeoPointDocValuesGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new SpatialExtentGeoPointDocValuesGroupingAggregatorFunction(channels, SpatialExtentGeoPointDocValuesAggregator.initGrouping(), driverContext);
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
    LongBlock valuesBlock = page.getBlock(channels.get(0));
    LongVector valuesVector = valuesBlock.asVector();
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

        @Override
        public void close() {
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

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        SpatialExtentGeoPointDocValuesAggregator.combine(state, groupId, values.getLong(v));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      SpatialExtentGeoPointDocValuesAggregator.combine(state, groupId, values.getLong(groupPosition + positionOffset));
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, LongBlock values) {
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
          SpatialExtentGeoPointDocValuesAggregator.combine(state, groupId, values.getLong(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, LongVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        SpatialExtentGeoPointDocValuesAggregator.combine(state, groupId, values.getLong(groupPosition + positionOffset));
      }
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block minNegXUncast = page.getBlock(channels.get(0));
    if (minNegXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minNegX = ((IntBlock) minNegXUncast).asVector();
    Block minPosXUncast = page.getBlock(channels.get(1));
    if (minPosXUncast.areAllValuesNull()) {
      return;
    }
    IntVector minPosX = ((IntBlock) minPosXUncast).asVector();
    Block maxNegXUncast = page.getBlock(channels.get(2));
    if (maxNegXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxNegX = ((IntBlock) maxNegXUncast).asVector();
    Block maxPosXUncast = page.getBlock(channels.get(3));
    if (maxPosXUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxPosX = ((IntBlock) maxPosXUncast).asVector();
    Block maxYUncast = page.getBlock(channels.get(4));
    if (maxYUncast.areAllValuesNull()) {
      return;
    }
    IntVector maxY = ((IntBlock) maxYUncast).asVector();
    Block minYUncast = page.getBlock(channels.get(5));
    if (minYUncast.areAllValuesNull()) {
      return;
    }
    IntVector minY = ((IntBlock) minYUncast).asVector();
    assert minNegX.getPositionCount() == minPosX.getPositionCount() && minNegX.getPositionCount() == maxNegX.getPositionCount() && minNegX.getPositionCount() == maxPosX.getPositionCount() && minNegX.getPositionCount() == maxY.getPositionCount() && minNegX.getPositionCount() == minY.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      SpatialExtentGeoPointDocValuesAggregator.combineIntermediate(state, groupId, minNegX.getInt(groupPosition + positionOffset), minPosX.getInt(groupPosition + positionOffset), maxNegX.getInt(groupPosition + positionOffset), maxPosX.getInt(groupPosition + positionOffset), maxY.getInt(groupPosition + positionOffset), minY.getInt(groupPosition + positionOffset));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    SpatialExtentGroupingStateWrappedLongitudeState inState = ((SpatialExtentGeoPointDocValuesGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    SpatialExtentGeoPointDocValuesAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      DriverContext driverContext) {
    blocks[offset] = SpatialExtentGeoPointDocValuesAggregator.evaluateFinal(state, selected, driverContext);
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
