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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctIntAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctIntAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("hll", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final HllStates.SingleState state;

  private final List<Integer> channels;

  private final int precision;

  public CountDistinctIntAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      HllStates.SingleState state, int precision) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.precision = precision;
  }

  public static CountDistinctIntAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int precision) {
    return new CountDistinctIntAggregatorFunction(driverContext, channels, CountDistinctIntAggregator.initSingle(driverContext.bigArrays(), precision), precision);
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
    IntBlock block = page.getBlock(channels.get(0));
    IntVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector);
    } else {
      addRawBlock(block);
    }
  }

  private void addRawVector(IntVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      CountDistinctIntAggregator.combine(state, vector.getInt(i));
    }
  }

  private void addRawBlock(IntBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        CountDistinctIntAggregator.combine(state, block.getInt(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block hllUncast = page.getBlock(channels.get(0));
    if (hllUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector hll = ((BytesRefBlock) hllUncast).asVector();
    assert hll.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    CountDistinctIntAggregator.combineIntermediate(state, hll.getBytesRef(0, scratch));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = CountDistinctIntAggregator.evaluateFinal(state, driverContext);
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
