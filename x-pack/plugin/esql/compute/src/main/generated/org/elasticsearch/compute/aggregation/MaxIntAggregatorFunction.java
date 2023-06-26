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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MaxIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxIntAggregatorFunction implements AggregatorFunction {
  private final IntState state;

  private final List<Integer> channels;

  public MaxIntAggregatorFunction(List<Integer> channels, IntState state) {
    this.channels = channels;
    this.state = state;
  }

  public static MaxIntAggregatorFunction create(List<Integer> channels) {
    return new MaxIntAggregatorFunction(channels, new IntState(MaxIntAggregator.init()));
  }

  @Override
  public void addRawInput(Page page) {
    Block uncastBlock = page.getBlock(channels.get(0));
    if (uncastBlock.areAllValuesNull()) {
      return;
    }
    IntBlock block = (IntBlock) uncastBlock;
    IntVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(IntVector vector) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.intValue(MaxIntAggregator.combine(state.intValue(), vector.getInt(i)));
    }
  }

  private void addRawBlock(IntBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        state.intValue(MaxIntAggregator.combine(state.intValue(), block.getInt(i)));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    Block block = page.getBlock(channels.get(0));
    Vector vector = block.asVector();
    if (vector == null || vector instanceof AggregatorStateVector == false) {
      throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
    }
    @SuppressWarnings("unchecked") AggregatorStateVector<IntState> blobVector = (AggregatorStateVector<IntState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    IntState tmpState = new IntState(MaxIntAggregator.init());
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.intValue(MaxIntAggregator.combine(state.intValue(), tmpState.intValue()));
    }
    state.seen(state.seen() || tmpState.seen());
    tmpState.close();
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    AggregatorStateVector.Builder<AggregatorStateVector<IntState>, IntState> builder =
        AggregatorStateVector.builderOfAggregatorState(IntState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    blocks[offset] = builder.build().asBlock();
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    if (state.seen() == false) {
      blocks[offset] = Block.constantNullBlock(1);
      return;
    }
    blocks[offset] = IntBlock.newConstantBlockWith(state.intValue(), 1);
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
