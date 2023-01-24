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
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MinDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private final DoubleArrayState state;

  private final int channel;

  public MinDoubleGroupingAggregatorFunction(int channel, DoubleArrayState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MinDoubleGroupingAggregatorFunction create(BigArrays bigArrays, int channel) {
    return new MinDoubleGroupingAggregatorFunction(channel, new DoubleArrayState(bigArrays, MinDoubleAggregator.init()));
  }

  @Override
  public void addRawInput(LongVector groupIdVector, Page page) {
    assert channel >= 0;
    DoubleBlock block = page.getBlock(channel);
    DoubleVector vector = block.asVector();
    if (vector != null) {
      addRawVector(groupIdVector, vector);
    } else {
      addRawBlock(groupIdVector, block);
    }
  }

  private void addRawVector(LongVector groupIdVector, DoubleVector vector) {
    for (int position = 0; position < vector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      state.set(MinDoubleAggregator.combine(state.getOrDefault(groupId), vector.getDouble(position)), groupId);
    }
  }

  private void addRawBlock(LongVector groupIdVector, DoubleBlock block) {
    for (int offset = 0; offset < block.getTotalValueCount(); offset++) {
      if (block.isNull(offset) == false) {
        int groupId = Math.toIntExact(groupIdVector.getLong(offset));
        state.set(MinDoubleAggregator.combine(state.getOrDefault(groupId), block.getDouble(offset)), groupId);
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
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleArrayState> blobVector = (AggregatorStateVector<DoubleArrayState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    DoubleArrayState inState = new DoubleArrayState(bigArrays, MinDoubleAggregator.init());
    blobVector.get(0, inState);
    for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
      int groupId = Math.toIntExact(groupIdVector.getLong(position));
      state.set(MinDoubleAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    DoubleArrayState inState = ((MinDoubleGroupingAggregatorFunction) input).state;
    state.set(MinDoubleAggregator.combine(state.getOrDefault(groupId), inState.get(position)), groupId);
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<DoubleArrayState>, DoubleArrayState> builder =
        AggregatorStateVector.builderOfAggregatorState(DoubleArrayState.class, state.getEstimatedSize());
    builder.add(state);
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    int positions = state.largestIndex + 1;
    double[] values = new double[positions];
    for (int i = 0; i < positions; i++) {
      values[i] = state.get(i);
    }
    return new DoubleArrayVector(values, positions).asBlock();
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
