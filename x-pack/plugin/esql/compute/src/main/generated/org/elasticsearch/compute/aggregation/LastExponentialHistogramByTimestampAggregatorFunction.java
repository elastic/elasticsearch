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
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * {@link AggregatorFunction} implementation for {@link LastExponentialHistogramByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class LastExponentialHistogramByTimestampAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final ExponentialHistogramStates.WithLongSingleState state;

  private final List<Integer> channels;

  public LastExponentialHistogramByTimestampAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, ExponentialHistogramStates.WithLongSingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static LastExponentialHistogramByTimestampAggregatorFunction create(
      DriverContext driverContext, List<Integer> channels) {
    return new LastExponentialHistogramByTimestampAggregatorFunction(driverContext, channels, LastExponentialHistogramByTimestampAggregator.initSingle(driverContext));
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
    ExponentialHistogramBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    addRawBlock(valueBlock, timestampBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    ExponentialHistogramBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock timestampBlock = page.getBlock(channels.get(1));
    addRawBlock(valueBlock, timestampBlock);
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock, LongBlock timestampBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
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
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          LastExponentialHistogramByTimestampAggregator.combine(state, valueValue, timestampValue);
        }
      }
    }
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock, LongBlock timestampBlock,
      BooleanVector mask) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
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
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          LastExponentialHistogramByTimestampAggregator.combine(state, valueValue, timestampValue);
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
    ExponentialHistogramBlock values = (ExponentialHistogramBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    ExponentialHistogramScratch valuesScratch = new ExponentialHistogramScratch();
    LastExponentialHistogramByTimestampAggregator.combineIntermediate(state, timestamps.getLong(0), values.getExponentialHistogram(values.getFirstValueIndex(0), valuesScratch), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = LastExponentialHistogramByTimestampAggregator.evaluateFinal(state, driverContext);
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
