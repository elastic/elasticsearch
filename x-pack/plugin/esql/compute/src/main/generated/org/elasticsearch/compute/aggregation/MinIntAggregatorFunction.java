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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MinIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinIntAggregatorFunction implements AggregatorFunction {
  private final IntState state;

  private final int channel;

  public MinIntAggregatorFunction(int channel, IntState state) {
    this.channel = channel;
    this.state = state;
  }

  public static MinIntAggregatorFunction create(int channel) {
    return new MinIntAggregatorFunction(channel, new IntState(MinIntAggregator.init()));
  }

  @Override
  public void addRawInput(Page page) {
    assert channel >= 0;
    ElementType type = page.getBlock(channel).elementType();
    if (type == ElementType.NULL) {
      return;
    }
    IntBlock block = page.getBlock(channel);
    IntVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(IntVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.intValue(MinIntAggregator.combine(state.intValue(), vector.getInt(i)));
    }
  }

  private void addRawBlock(IntBlock block) {
    for (int p = 0; p < block.getTotalValueCount(); p++) {
      if (block.isNull(p) == false) {
        int i = block.getFirstValueIndex(p);
        state.intValue(MinIntAggregator.combine(state.intValue(), block.getInt(i)));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<IntState> blobVector = (AggregatorStateVector<IntState>) vector;
    IntState tmpState = new IntState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.intValue(MinIntAggregator.combine(state.intValue(), tmpState.intValue()));
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<IntState>, IntState> builder =
        AggregatorStateVector.builderOfAggregatorState(IntState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return IntBlock.newConstantBlockWith(state.intValue(), 1);
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
