// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MedianAbsoluteDeviationLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MedianAbsoluteDeviationLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final QuantileStates.GroupingState state;

  private final int channel;

  public MedianAbsoluteDeviationLongGroupingAggregatorFunction(int channel,
      QuantileStates.GroupingState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MedianAbsoluteDeviationLongGroupingAggregatorFunction create(BigArrays bigArrays,
      int channel) {
    return new MedianAbsoluteDeviationLongGroupingAggregatorFunction(channel, MedianAbsoluteDeviationLongAggregator.initGrouping(bigArrays));
  }

  @Override
  public void addRawInput(LongVector groupIdVector, Page page) {
    assert channel >= 0;
    LongBlock block = page.getBlock(channel);
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(groupIdVector, vector);
    } else {
      addRawBlock(groupIdVector, block);
    }
  }

  private void addRawVector(LongVector groupIdVector, LongVector vector) {
    for (int position = 0; position < vector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      MedianAbsoluteDeviationLongAggregator.combine(state, groupId, vector.getLong(position));
    }
  }

  private void addRawBlock(LongVector groupIdVector, LongBlock block) {
    for (int offset = 0; offset < block.getTotalValueCount(); offset++) {
      if (block.isNull(offset) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(offset));
        MedianAbsoluteDeviationLongAggregator.combine(state, groupId, block.getLong(offset));
      }
    }
  }

  @Override
  public void addIntermediateInput(LongVector groupIdVector, Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<QuantileStates.GroupingState> blobVector = (AggregatorStateVector<QuantileStates.GroupingState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    QuantileStates.GroupingState inState = MedianAbsoluteDeviationLongAggregator.initGrouping(bigArrays);
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      MedianAbsoluteDeviationLongAggregator.combineStates(state, groupId, inState, position);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    QuantileStates.GroupingState inState = ((MedianAbsoluteDeviationLongGroupingAggregatorFunction) input).state;
    MedianAbsoluteDeviationLongAggregator.combineStates(state, groupId, inState, position);
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<QuantileStates.GroupingState>, QuantileStates.GroupingState> builder =
        AggregatorStateVector.builderOfAggregatorState(QuantileStates.GroupingState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return MedianAbsoluteDeviationLongAggregator.evaluateFinal(state);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channel=").append(channel);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
