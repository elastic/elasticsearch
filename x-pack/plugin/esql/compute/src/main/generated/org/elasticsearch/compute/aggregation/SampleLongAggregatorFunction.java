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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link SampleLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SampleLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sample", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final SampleLongAggregator.SingleState state;

  private final List<Integer> channels;

  private final int limit;

  public SampleLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      SampleLongAggregator.SingleState state, int limit) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.limit = limit;
  }

  public static SampleLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int limit) {
    return new SampleLongAggregatorFunction(driverContext, channels, SampleLongAggregator.initSingle(driverContext.bigArrays(), limit), limit);
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
    } else if (mask.allTrue()) {
      addRawInputNotMasked(page);
    } else {
      addRawInputMasked(page, mask);
    }
  }

  private void addRawInputMasked(Page page, BooleanVector mask) {
    LongBlock valueBlock = page.getBlock(channels.get(0));
    LongVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock, mask);
      return;
    }
    addRawVector(valueVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    LongBlock valueBlock = page.getBlock(channels.get(0));
    LongVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock);
      return;
    }
    addRawVector(valueVector);
  }

  private void addRawVector(LongVector valueVector) {
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      long valueValue = valueVector.getLong(valuesPosition);
      SampleLongAggregator.combine(state, valueValue);
    }
  }

  private void addRawVector(LongVector valueVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long valueValue = valueVector.getLong(valuesPosition);
      SampleLongAggregator.combine(state, valueValue);
    }
  }

  private void addRawBlock(LongBlock valueBlock) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        long valueValue = valueBlock.getLong(valueOffset);
        SampleLongAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(LongBlock valueBlock, BooleanVector mask) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        long valueValue = valueBlock.getLong(valueOffset);
        SampleLongAggregator.combine(state, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block sampleUncast = page.getBlock(channels.get(0));
    if (sampleUncast.areAllValuesNull()) {
      return;
    }
    BytesRefBlock sample = (BytesRefBlock) sampleUncast;
    assert sample.getPositionCount() == 1;
    BytesRef sampleScratch = new BytesRef();
    SampleLongAggregator.combineIntermediate(state, sample);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SampleLongAggregator.evaluateFinal(state, driverContext);
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
