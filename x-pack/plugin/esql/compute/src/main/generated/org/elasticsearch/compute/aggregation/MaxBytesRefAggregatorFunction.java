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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link MaxBytesRefAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class MaxBytesRefAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("max", ElementType.BYTES_REF),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final MaxBytesRefAggregator.SingleState state;

  private final List<Integer> channels;

  public MaxBytesRefAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      MaxBytesRefAggregator.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static MaxBytesRefAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new MaxBytesRefAggregatorFunction(driverContext, channels, MaxBytesRefAggregator.initSingle(driverContext));
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
    BytesRefBlock valueBlock = page.getBlock(channels.get(0));
    BytesRefVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock, mask);
      return;
    }
    addRawVector(valueVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    BytesRefBlock valueBlock = page.getBlock(channels.get(0));
    BytesRefVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock);
      return;
    }
    addRawVector(valueVector);
  }

  private void addRawVector(BytesRefVector valueVector) {
    BytesRef valueScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      BytesRef valueValue = valueVector.getBytesRef(valuesPosition, valueScratch);
      MaxBytesRefAggregator.combine(state, valueValue);
    }
  }

  private void addRawVector(BytesRefVector valueVector, BooleanVector mask) {
    BytesRef valueScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      BytesRef valueValue = valueVector.getBytesRef(valuesPosition, valueScratch);
      MaxBytesRefAggregator.combine(state, valueValue);
    }
  }

  private void addRawBlock(BytesRefBlock valueBlock) {
    BytesRef valueScratch = new BytesRef();
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        BytesRef valueValue = valueBlock.getBytesRef(valueOffset, valueScratch);
        MaxBytesRefAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(BytesRefBlock valueBlock, BooleanVector mask) {
    BytesRef valueScratch = new BytesRef();
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
        BytesRef valueValue = valueBlock.getBytesRef(valueOffset, valueScratch);
        MaxBytesRefAggregator.combine(state, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block maxUncast = page.getBlock(channels.get(0));
    if (maxUncast.areAllValuesNull()) {
      return;
    }
    BytesRefVector max = ((BytesRefBlock) maxUncast).asVector();
    assert max.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    BytesRef maxScratch = new BytesRef();
    MaxBytesRefAggregator.combineIntermediate(state, max.getBytesRef(0, maxScratch), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = MaxBytesRefAggregator.evaluateFinal(state, driverContext);
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
