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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link PercentileIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class PercentileIntGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("quart", ElementType.BYTES_REF)  );

  private final QuantileStates.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final double percentile;

  public PercentileIntGroupingAggregatorFunction(List<Integer> channels,
      QuantileStates.GroupingState state, DriverContext driverContext, double percentile) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
    this.percentile = percentile;
  }

  public static PercentileIntGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext, double percentile) {
    return new PercentileIntGroupingAggregatorFunction(channels, PercentileIntAggregator.initGrouping(driverContext.bigArrays(), percentile), driverContext, percentile);
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

  private void addRawInput(int positionOffset, IntVector groups, IntBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getInt(groupPosition));
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        PercentileIntAggregator.combine(state, groupId, values.getInt(v));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, IntVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getInt(groupPosition));
      PercentileIntAggregator.combine(state, groupId, values.getInt(groupPosition + positionOffset));
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, IntBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getInt(g));
        if (values.isNull(groupPosition + positionOffset)) {
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          PercentileIntAggregator.combine(state, groupId, values.getInt(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, IntVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = Math.toIntExact(groups.getInt(g));
        PercentileIntAggregator.combine(state, groupId, values.getInt(groupPosition + positionOffset));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block quartUncast = page.getBlock(channels.get(0));
    if (quartUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector quart = ((BytesRefBlock) quartUncast).asVector();
    BytesRef scratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = Math.toIntExact(groups.getInt(groupPosition));
      PercentileIntAggregator.combineIntermediate(state, groupId, quart.getBytesRef(groupPosition + positionOffset, scratch));
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    QuantileStates.GroupingState inState = ((PercentileIntGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    PercentileIntAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      DriverContext driverContext) {
    blocks[offset] = PercentileIntAggregator.evaluateFinal(state, selected, driverContext);
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
