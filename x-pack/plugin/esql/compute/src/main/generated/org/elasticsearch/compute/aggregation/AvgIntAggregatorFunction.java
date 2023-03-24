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
 * {@link AggregatorFunction} implementation for {@link AvgIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class AvgIntAggregatorFunction implements AggregatorFunction {
  private final AvgLongAggregator.AvgState state;

  private final int channel;

  public AvgIntAggregatorFunction(int channel, AvgLongAggregator.AvgState state) {
    this.channel = channel;
    this.state = state;
  }

  public static AvgIntAggregatorFunction create(int channel) {
    return new AvgIntAggregatorFunction(channel, AvgIntAggregator.initSingle());
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
      AvgIntAggregator.combine(state, vector.getInt(i));
    }
    AvgIntAggregator.combineValueCount(state, vector.getPositionCount());
  }

  private void addRawBlock(IntBlock block) {
    for (int p = 0; p < block.getTotalValueCount(); p++) {
      if (block.isNull(p) == false) {
        int i = block.getFirstValueIndex(p);
        AvgIntAggregator.combine(state, block.getInt(i));
      }
    }
    AvgIntAggregator.combineValueCount(state, block.validPositionCount());
  }

  @Override
  public void addIntermediateInput(Block block) {
    assert channel == -1;
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<AvgLongAggregator.AvgState> blobVector = (AggregatorStateVector<AvgLongAggregator.AvgState>) vector;
    AvgLongAggregator.AvgState tmpState = new AvgLongAggregator.AvgState();
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      AvgIntAggregator.combineStates(state, tmpState);
    }
  }

  @Override
  public Block evaluateIntermediate() {
    AggregatorStateVector.Builder<AggregatorStateVector<AvgLongAggregator.AvgState>, AvgLongAggregator.AvgState> builder =
        AggregatorStateVector.builderOfAggregatorState(AvgLongAggregator.AvgState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    return builder.build().asBlock();
  }

  @Override
  public Block evaluateFinal() {
    return AvgIntAggregator.evaluateFinal(state);
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
