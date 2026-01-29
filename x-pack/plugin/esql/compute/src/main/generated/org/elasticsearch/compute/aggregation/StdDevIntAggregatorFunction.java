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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link StdDevIntAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class StdDevIntAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("mean", ElementType.DOUBLE),
      new IntermediateStateDesc("m2", ElementType.DOUBLE),
      new IntermediateStateDesc("count", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final VarianceStates.SingleState state;

  private final List<Integer> channels;

  private final boolean stdDev;

  public StdDevIntAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      VarianceStates.SingleState state, boolean stdDev) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.stdDev = stdDev;
  }

  public static StdDevIntAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, boolean stdDev) {
    return new StdDevIntAggregatorFunction(driverContext, channels, StdDevIntAggregator.initSingle(stdDev), stdDev);
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
    IntBlock valueBlock = page.getBlock(channels.get(0));
    IntVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock, mask);
      return;
    }
    addRawVector(valueVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    IntBlock valueBlock = page.getBlock(channels.get(0));
    IntVector valueVector = valueBlock.asVector();
    if (valueVector == null) {
      addRawBlock(valueBlock);
      return;
    }
    addRawVector(valueVector);
  }

  private void addRawVector(IntVector valueVector) {
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      int valueValue = valueVector.getInt(valuesPosition);
      StdDevIntAggregator.combine(state, valueValue);
    }
  }

  private void addRawVector(IntVector valueVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < valueVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      int valueValue = valueVector.getInt(valuesPosition);
      StdDevIntAggregator.combine(state, valueValue);
    }
  }

  private void addRawBlock(IntBlock valueBlock) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        int valueValue = valueBlock.getInt(valueOffset);
        StdDevIntAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(IntBlock valueBlock, BooleanVector mask) {
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
        int valueValue = valueBlock.getInt(valueOffset);
        StdDevIntAggregator.combine(state, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block meanUncast = page.getBlock(channels.get(0));
    if (meanUncast.areAllValuesNull()) {
      return;
    }
    DoubleVector mean = ((DoubleBlock) meanUncast).asVector();
    assert mean.getPositionCount() == 1;
    Block m2Uncast = page.getBlock(channels.get(1));
    if (m2Uncast.areAllValuesNull()) {
      return;
    }
    DoubleVector m2 = ((DoubleBlock) m2Uncast).asVector();
    assert m2.getPositionCount() == 1;
    Block countUncast = page.getBlock(channels.get(2));
    if (countUncast.areAllValuesNull()) {
      return;
    }
    LongVector count = ((LongBlock) countUncast).asVector();
    assert count.getPositionCount() == 1;
    StdDevIntAggregator.combineIntermediate(state, mean.getDouble(0), m2.getDouble(0), count.getLong(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = StdDevIntAggregator.evaluateFinal(state, driverContext);
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
