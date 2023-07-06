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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

/**
 * {@link AggregatorFunction} implementation for {@link MinLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MinLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("min", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final LongState state;

  private final List<Integer> channels;

  public MinLongAggregatorFunction(List<Integer> channels, LongState state) {
    this.channels = channels;
    this.state = state;
  }

  public static MinLongAggregatorFunction create(List<Integer> channels) {
    return new MinLongAggregatorFunction(channels, new LongState(MinLongAggregator.init()));
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page) {
    Block uncastBlock = page.getBlock(channels.get(0));
    if (uncastBlock.areAllValuesNull()) {
      return;
    }
    LongBlock block = (LongBlock) uncastBlock;
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(LongVector vector) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      state.longValue(MinLongAggregator.combine(state.longValue(), vector.getLong(i)));
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        state.longValue(MinLongAggregator.combine(state.longValue(), block.getLong(i)));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    LongVector min = page.<LongBlock>getBlock(channels.get(0)).asVector();
    BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
    assert min.getPositionCount() == 1;
    assert min.getPositionCount() == seen.getPositionCount();
    if (seen.getBoolean(0)) {
      state.longValue(MinLongAggregator.combine(state.longValue(), min.getLong(0)));
      state.seen(true);
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    state.toIntermediate(blocks, offset);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    if (state.seen() == false) {
      blocks[offset] = Block.constantNullBlock(1);
      return;
    }
    blocks[offset] = LongBlock.newConstantBlockWith(state.longValue(), 1);
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
