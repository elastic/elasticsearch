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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class CountDistinctLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("hll", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final HllStates.SingleState state;

  private final List<Integer> channels;

  private final int precision;

  public CountDistinctLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      HllStates.SingleState state, int precision) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.precision = precision;
  }

  public static CountDistinctLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int precision) {
    return new CountDistinctLongAggregatorFunction(driverContext, channels, CountDistinctLongAggregator.initSingle(driverContext.bigArrays(), precision), precision);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page, BooleanVector mask) {
    if (mask.allFalse()) {
      // Entire page masked away
      return;
    }
    if (mask.allTrue()) {
      // No masking
      LongBlock block = page.getBlock(channels.get(0));
      LongVector vector = block.asVector();
      if (vector != null) {
        addRawVector(vector);
      } else {
        addRawBlock(block);
      }
      return;
    }
    // Some positions masked away, others kept
    LongBlock block = page.getBlock(channels.get(0));
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector, mask);
    } else {
      addRawBlock(block, mask);
    }
  }

  private void addRawVector(LongVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      CountDistinctLongAggregator.combine(state, vector.getLong(i));
    }
  }

  private void addRawVector(LongVector vector, BooleanVector mask) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
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

  private void addRawBlock(LongBlock block, BooleanVector mask) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
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
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block hllUncast = page.getBlock(channels.get(0));
    if (hllUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector hll = ((BytesRefBlock) hllUncast).asVector();
    assert hll.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    CountDistinctLongAggregator.combineIntermediate(state, hll.getBytesRef(0, scratch));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = CountDistinctLongAggregator.evaluateFinal(state, driverContext);
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
