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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link MedianAbsoluteDeviationFloatAggregator}.
 * This class is generated. Do not edit it.
 */
public final class MedianAbsoluteDeviationFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("quart", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final QuantileStates.SingleState state;

  private final List<Integer> channels;

  public MedianAbsoluteDeviationFloatAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, QuantileStates.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static MedianAbsoluteDeviationFloatAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new MedianAbsoluteDeviationFloatAggregatorFunction(driverContext, channels, MedianAbsoluteDeviationFloatAggregator.initSingle(driverContext));
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
      FloatBlock block = page.getBlock(channels.get(0));
      FloatVector vector = block.asVector();
      if (vector != null) {
        addRawVector(vector);
      } else {
        addRawBlock(block);
      }
      return;
    }
    // Some positions masked away, others kept
    FloatBlock block = page.getBlock(channels.get(0));
    FloatVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector, mask);
    } else {
      addRawBlock(block, mask);
    }
  }

  private void addRawVector(FloatVector vector) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      MedianAbsoluteDeviationFloatAggregator.combine(state, vector.getFloat(i));
    }
  }

  private void addRawVector(FloatVector vector, BooleanVector mask) {
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      MedianAbsoluteDeviationFloatAggregator.combine(state, vector.getFloat(i));
    }
  }

  private void addRawBlock(FloatBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        MedianAbsoluteDeviationFloatAggregator.combine(state, block.getFloat(i));
      }
    }
  }

  private void addRawBlock(FloatBlock block, BooleanVector mask) {
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
        MedianAbsoluteDeviationFloatAggregator.combine(state, block.getFloat(i));
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block quartUncast = page.getBlock(channels.get(0));
    if (quartUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector quart = ((BytesRefBlock) quartUncast).asVector();
    assert quart.getPositionCount() == 1;
    BytesRef scratch = new BytesRef();
    MedianAbsoluteDeviationFloatAggregator.combineIntermediate(state, quart.getBytesRef(0, scratch));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = MedianAbsoluteDeviationFloatAggregator.evaluateFinal(state, driverContext);
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
