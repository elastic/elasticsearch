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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialExtentGeoShapeDocValuesAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SpatialExtentGeoShapeDocValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.INT),
      new IntermediateStateDesc("bottom", ElementType.INT),
      new IntermediateStateDesc("negLeft", ElementType.INT),
      new IntermediateStateDesc("negRight", ElementType.INT),
      new IntermediateStateDesc("posLeft", ElementType.INT),
      new IntermediateStateDesc("posRight", ElementType.INT)  );

  private final SpatialExtentGroupingStateWrappedLongitudeState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SpatialExtentGeoShapeDocValuesGroupingAggregatorFunction(List<Integer> channels,
      SpatialExtentGroupingStateWrappedLongitudeState state, DriverContext driverContext) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SpatialExtentGeoShapeDocValuesGroupingAggregatorFunction create(
      List<Integer> channels, DriverContext driverContext) {
    return new SpatialExtentGeoShapeDocValuesGroupingAggregatorFunction(channels, SpatialExtentGeoShapeDocValuesAggregator.initGrouping(), driverContext);
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
    IntBlock valuesBlock = page.getBlock(channels.get(0));
    IntVector valuesVector = valuesBlock.asVector();
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

  private void addRawInput(int positionOffset, IntVector groups, IntBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      int[] valuesArray = new int[valuesEnd - valuesStart];
      for (int v = valuesStart; v < valuesEnd; v++) {
        valuesArray[v-valuesStart] = values.getInt(v);
      }
      SpatialExtentGeoShapeDocValuesAggregator.combine(state, groupId, valuesArray);
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, IntVector values) {
    // This type does not support vectors because all values are multi-valued
  }

  private void addRawInput(int positionOffset, IntBlock groups, IntBlock values) {
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
        int[] valuesArray = new int[valuesEnd - valuesStart];
        for (int v = valuesStart; v < valuesEnd; v++) {
          valuesArray[v-valuesStart] = values.getInt(v);
        }
        SpatialExtentGeoShapeDocValuesAggregator.combine(state, groupId, valuesArray);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, IntVector values) {
    // This type does not support vectors because all values are multi-valued
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    IntVector top = ((IntBlock) topUncast).asVector();
    Block bottomUncast = page.getBlock(channels.get(1));
    if (bottomUncast.areAllValuesNull()) {
      return;
    }
    IntVector bottom = ((IntBlock) bottomUncast).asVector();
    Block negLeftUncast = page.getBlock(channels.get(2));
    if (negLeftUncast.areAllValuesNull()) {
      return;
    }
    IntVector negLeft = ((IntBlock) negLeftUncast).asVector();
    Block negRightUncast = page.getBlock(channels.get(3));
    if (negRightUncast.areAllValuesNull()) {
      return;
    }
    IntVector negRight = ((IntBlock) negRightUncast).asVector();
    Block posLeftUncast = page.getBlock(channels.get(4));
    if (posLeftUncast.areAllValuesNull()) {
      return;
    }
    IntVector posLeft = ((IntBlock) posLeftUncast).asVector();
    Block posRightUncast = page.getBlock(channels.get(5));
    if (posRightUncast.areAllValuesNull()) {
      return;
    }
    IntVector posRight = ((IntBlock) posRightUncast).asVector();
    assert top.getPositionCount() == bottom.getPositionCount() && top.getPositionCount() == negLeft.getPositionCount() && top.getPositionCount() == negRight.getPositionCount() && top.getPositionCount() == posLeft.getPositionCount() && top.getPositionCount() == posRight.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      SpatialExtentGeoShapeDocValuesAggregator.combineIntermediate(state, groupId, top.getInt(groupPosition + positionOffset), bottom.getInt(groupPosition + positionOffset), negLeft.getInt(groupPosition + positionOffset), negRight.getInt(groupPosition + positionOffset), posLeft.getInt(groupPosition + positionOffset), posRight.getInt(groupPosition + positionOffset));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    SpatialExtentGroupingStateWrappedLongitudeState inState = ((SpatialExtentGeoShapeDocValuesGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    SpatialExtentGeoShapeDocValuesAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      DriverContext driverContext) {
    blocks[offset] = SpatialExtentGeoShapeDocValuesAggregator.evaluateFinal(state, selected, driverContext);
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
