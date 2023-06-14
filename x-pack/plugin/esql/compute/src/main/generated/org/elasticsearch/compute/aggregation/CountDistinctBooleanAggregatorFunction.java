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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctBooleanAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctBooleanAggregatorFunction implements AggregatorFunction {
  private final CountDistinctBooleanAggregator.SingleState state;

  private final int channel;

  public CountDistinctBooleanAggregatorFunction(int channel,
      CountDistinctBooleanAggregator.SingleState state) {
    this.channel = channel;
    this.state = state;
  }

  public static CountDistinctBooleanAggregatorFunction create(int channel) {
    return new CountDistinctBooleanAggregatorFunction(channel, CountDistinctBooleanAggregator.initSingle());
  }

  @Override
  public void addRawInput(Page page) {
    ElementType type = page.getBlock(channel).elementType();
    if (type == ElementType.NULL) {
      return;
    }
    BooleanBlock block = page.getBlock(channel);
    BooleanVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(BooleanVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      CountDistinctBooleanAggregator.combine(state, vector.getBoolean(i));
    }
  }

  private void addRawBlock(BooleanBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        CountDistinctBooleanAggregator.combine(state, block.getBoolean(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Block block) {
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<CountDistinctBooleanAggregator.SingleState> blobVector = (AggregatorStateVector<CountDistinctBooleanAggregator.SingleState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    CountDistinctBooleanAggregator.SingleState tmpState = CountDistinctBooleanAggregator.initSingle();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      CountDistinctBooleanAggregator.combineStates(state, tmpState);
    }
    tmpState.close();
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<CountDistinctBooleanAggregator.SingleState>, CountDistinctBooleanAggregator.SingleState> builder =
        AggregatorStateVector.builderOfAggregatorState(CountDistinctBooleanAggregator.SingleState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return CountDistinctBooleanAggregator.evaluateFinal(state);
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
