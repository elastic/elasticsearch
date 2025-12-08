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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link DerivLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class DerivLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("count", ElementType.LONG),
      new IntermediateStateDesc("sumVal", ElementType.DOUBLE),
      new IntermediateStateDesc("sumTs", ElementType.LONG),
      new IntermediateStateDesc("sumTsVal", ElementType.DOUBLE),
      new IntermediateStateDesc("sumTsSq", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final SimpleLinearRegressionWithTimeseries state;

  private final List<Integer> channels;

  public DerivLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      SimpleLinearRegressionWithTimeseries state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static DerivLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new DerivLongAggregatorFunction(driverContext, channels, DerivLongAggregator.initSingle(driverContext));
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
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      DerivLongAggregator.combine(state, valueValue, timestampValue);
    }
  }

  private void addRawVector(LongVector valueVector, LongVector timestampVector,
      BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long valueValue = valueVector.getLong(valuesPosition);
      long timestampValue = timestampVector.getLong(valuesPosition);
      DerivLongAggregator.combine(state, valueValue, timestampValue);
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
          DerivLongAggregator.combine(state, valueValue, timestampValue);
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
          DerivLongAggregator.combine(state, valueValue, timestampValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block countUncast = page.getBlock(channels.get(0));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    assert count.getPositionCount() == 1;
    Block sumValUncast = page.getBlock(channels.get(1));
    if (sumValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumVal = ((DoubleBlock) sumValUncast).asVector();
    assert sumVal.getPositionCount() == 1;
    Block sumTsUncast = page.getBlock(channels.get(2));
    if (sumTsUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTs = ((LongBlock) sumTsUncast).asVector();
    assert sumTs.getPositionCount() == 1;
    Block sumTsValUncast = page.getBlock(channels.get(3));
    if (sumTsValUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector sumTsVal = ((DoubleBlock) sumTsValUncast).asVector();
    assert sumTsVal.getPositionCount() == 1;
    Block sumTsSqUncast = page.getBlock(channels.get(4));
    if (sumTsSqUncast.areAllValuesNull()) {
      return;
    }
    LongVector sumTsSq = ((LongBlock) sumTsSqUncast).asVector();
    assert sumTsSq.getPositionCount() == 1;
    DerivLongAggregator.combineIntermediate(state, count.getLong(0), sumVal.getDouble(0), sumTs.getLong(0), sumTsVal.getDouble(0), sumTsSq.getLong(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = DerivLongAggregator.evaluateFinal(state, driverContext);
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
