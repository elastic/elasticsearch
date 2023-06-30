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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("aggstate", ElementType.UNKNOWN)  );

  private final HllStates.SingleState state;

  private final List<Integer> channels;

  private final BigArrays bigArrays;

  private final int precision;

  public CountDistinctLongAggregatorFunction(List<Integer> channels, HllStates.SingleState state,
      BigArrays bigArrays, int precision) {
    this.channels = channels;
    this.state = state;
    this.bigArrays = bigArrays;
    this.precision = precision;
  }

  public static CountDistinctLongAggregatorFunction create(List<Integer> channels,
      BigArrays bigArrays, int precision) {
    return new CountDistinctLongAggregatorFunction(channels, CountDistinctLongAggregator.initSingle(bigArrays, precision), bigArrays, precision);
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
    for (int i = 0; i < vector.getPositionCount(); i++) {
      CountDistinctLongAggregator.combine(state, vector.getLong(i));
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        CountDistinctLongAggregator.combine(state, block.getLong(i));
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
    @SuppressWarnings("unchecked") AggregatorStateVector<HllStates.SingleState> blobVector = (AggregatorStateVector<HllStates.SingleState>) vector;
    // TODO exchange big arrays directly without funny serialization - no more copying
    BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    HllStates.SingleState tmpState = CountDistinctLongAggregator.initSingle(bigArrays, precision);
    for (int i = 0; i < block.getPositionCount(); i++) {
      blobVector.get(i, tmpState);
      CountDistinctLongAggregator.combineStates(state, tmpState);
    }
    tmpState.close();
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset) {
    AggregatorStateVector.Builder<AggregatorStateVector<HllStates.SingleState>, HllStates.SingleState> builder =
        AggregatorStateVector.builderOfAggregatorState(HllStates.SingleState.class, state.getEstimatedSize());
    builder.add(state, IntVector.range(0, 1));
    blocks[offset] = builder.build().asBlock();
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset) {
    blocks[offset] = CountDistinctLongAggregator.evaluateFinal(state);
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
