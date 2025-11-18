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
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link FirstLongByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class FirstLongByTimestampAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final LongLongState state;

  private final List<Integer> channels;

  public FirstLongByTimestampAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      LongLongState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static FirstLongByTimestampAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new FirstLongByTimestampAggregatorFunction(driverContext, channels, FirstLongByTimestampAggregator.initSingle(driverContext));
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
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    LongVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock, timestampBlock, mask);
      return;
    }
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      addRawBlock(valueBlock, timestampBlock, mask);
      return;
    }
    addRawVector(valueVector, timestampVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    LongBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    LongVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock, timestampBlock);
      return;
    }
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      addRawBlock(valueBlock, timestampBlock);
      return;
    }
    addRawVector(valueVector, timestampVector);
  }

  private void addRawVector(LongVector valueVector, LongVector timestampVector) {
    // Find the first value up front in the Vector path which is more complex but should be faster
    int valuesPosition = 0;
    while (state.seen() == false && valuesPosition < valueVector.getPositionCount()) {
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      FirstLongByTimestampAggregator.first(state, valueValue, timestampValue);
      valuesPosition++;
      state.seen(true);
      break;
    }
    while (valuesPosition < valueVector.getPositionCount()) {
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      FirstLongByTimestampAggregator.combine(state, valueValue, timestampValue);
      valuesPosition++;
    }
  }

  private void addRawVector(LongVector valueVector, LongVector timestampVector,
      BooleanVector mask) {
    // Find the first value up front in the Vector path which is more complex but should be faster
    int valuesPosition = 0;
    while (state.seen() == false && valuesPosition < valueVector.getPositionCount()) {
      if (mask.getBoolean(valuesPosition) == false) {
        valuesPosition++;
        continue;
      }
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      FirstLongByTimestampAggregator.first(state, valueValue, timestampValue);
      valuesPosition++;
      state.seen(true);
      break;
    }
    while (valuesPosition < valueVector.getPositionCount()) {
      if (mask.getBoolean(valuesPosition) == false) {
        valuesPosition++;
        continue;
      }
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      FirstLongByTimestampAggregator.combine(state, valueValue, timestampValue);
      valuesPosition++;
    }
  }

  private void addRawBlock(LongBlock valueBlock, LongBlock timestampBlock) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        long valueValue = valueBlock.getLong(valueOffset);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          // Check seen in every iteration to save on complexity in the Block path
          if (state.seen()) {
            FirstLongByTimestampAggregator.combine(state, valueValue, timestampValue);
          } else {
            state.seen(true);
            FirstLongByTimestampAggregator.first(state, valueValue, timestampValue);
          }
        }
      }
    }
  }

  private void addRawBlock(LongBlock valueBlock, LongBlock timestampBlock, BooleanVector mask) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        long valueValue = valueBlock.getLong(valueOffset);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          // Check seen in every iteration to save on complexity in the Block path
          if (state.seen()) {
            FirstLongByTimestampAggregator.combine(state, valueValue, timestampValue);
          } else {
            state.seen(true);
            FirstLongByTimestampAggregator.first(state, valueValue, timestampValue);
          }
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
      return;
    }
    LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
    assert timestamps.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    LongVector values = ((LongBlock) valuesUncast).asVector();
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    FirstLongByTimestampAggregator.combineIntermediate(state, timestamps.getLong(0), values.getLong(0), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    if (state.seen() == false) {
      blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
      return;
    }
    blocks[offset] = FirstLongByTimestampAggregator.evaluateFinal(state, driverContext);
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
