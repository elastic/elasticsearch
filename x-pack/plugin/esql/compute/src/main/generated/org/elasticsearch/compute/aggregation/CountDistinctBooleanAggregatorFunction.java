// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
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
    assert channel >= 0;
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
    for (int p = 0; p < block.getTotalValueCount(); p++) {
      if (block.isNull(p) == false) {
        int i = block.getFirstValueIndex(p);
        CountDistinctBooleanAggregator.combine(state, block.getBoolean(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<CountDistinctBooleanAggregator.SingleState> blobVector = (AggregatorStateVector<CountDistinctBooleanAggregator.SingleState>) vector;
    CountDistinctBooleanAggregator.SingleState tmpState = new CountDistinctBooleanAggregator.SingleState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      CountDistinctBooleanAggregator.combineStates(state, tmpState);
    }
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
}
