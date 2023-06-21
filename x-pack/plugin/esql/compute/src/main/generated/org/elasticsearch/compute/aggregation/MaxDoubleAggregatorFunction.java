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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link MaxDoubleAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MaxDoubleAggregatorFunction implements AggregatorFunction {
  private final DoubleState state;

  private final List<Integer> channels;

  public MaxDoubleAggregatorFunction(List<Integer> channels, DoubleState state) {
    this.channels = channels;
    this.state = state;
  }

  public static MaxDoubleAggregatorFunction create(List<Integer> channels) {
    return new MaxDoubleAggregatorFunction(channels, new DoubleState(MaxDoubleAggregator.init()));
  }

  @Override
  public void addRawInput(Page page) {
    ElementType type = page.getBlock(channels.get(0)).elementType();
    if (type == ElementType.NULL) {
      return;
    }
    DoubleBlock block = page.getBlock(channels.get(0));
    DoubleVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(DoubleVector vector) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), vector.getDouble(i)));
    }
  }

  private void addRawBlock(DoubleBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), block.getDouble(i)));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<DoubleState> blobVector = (AggregatorStateVector<DoubleState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    DoubleState tmpState = new DoubleState(MaxDoubleAggregator.init());
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), tmpState.doubleValue()));
    }
    state.seen(state.seen() || tmpState.seen());
    tmpState.close();
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    AggregatorStateVector.Builder<AggregatorStateVector<DoubleState>, DoubleState> builder =
        AggregatorStateVector.builderOfAggregatorState(DoubleState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    blocks[offset] = builder.build().asBlock();
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    if (state.seen() == false) {
      blocks[offset] = Block.constantNullBlock(1);
      return;
    }
    blocks[offset] = DoubleBlock.newConstantBlockWith(state.doubleValue(), 1);
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
