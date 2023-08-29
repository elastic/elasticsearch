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
import org.elasticsearch.compute.data.Page;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctBooleanAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctBooleanAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("fbit", ElementType.BOOLEAN),
      new IntermediateStateDesc("tbit", ElementType.BOOLEAN)  );

  private final CountDistinctBooleanAggregator.SingleState state;

  private final List<Integer> channels;

  public CountDistinctBooleanAggregatorFunction(List<Integer> channels,
      CountDistinctBooleanAggregator.SingleState state) {
    this.channels = channels;
    this.state = state;
  }

  public static CountDistinctBooleanAggregatorFunction create(List<Integer> channels) {
    return new CountDistinctBooleanAggregatorFunction(channels, CountDistinctBooleanAggregator.initSingle());
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
    BooleanBlock block = (BooleanBlock) uncastBlock;
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
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    BooleanVector fbit = page.<BooleanBlock>getBlock(channels.get(0)).asVector();
    BooleanVector tbit = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
    assert fbit.getPositionCount() == 1;
    assert fbit.getPositionCount() == tbit.getPositionCount();
    CountDistinctBooleanAggregator.combineIntermediate(state, fbit.getBoolean(0), tbit.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    state.toIntermediate(blocks, offset);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    blocks[offset] = CountDistinctBooleanAggregator.evaluateFinal(state);
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
