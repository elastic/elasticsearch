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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * {@link AggregatorFunction} implementation for {@link AllFirstExponentialHistogramByLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class AllFirstExponentialHistogramByLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sortKeys", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.EXPONENTIAL_HISTOGRAM),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final ExponentialHistogramStates.WithLongSingleState state;

  private final List<Integer> channels;

  AllFirstExponentialHistogramByLongAggregatorFunction(DriverContext driverContext,
      List<Integer> channels) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = AllFirstExponentialHistogramByLongAggregator.initSingle(driverContext);
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
    LongBlock sortKeyBlock = page.getBlock(channels.get(1));
    addRawBlock(valueBlock, sortKeyBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    ExponentialHistogramBlock valueBlock = page.getBlock(channels.get(0));
    LongBlock sortKeyBlock = page.getBlock(channels.get(1));
    addRawBlock(valueBlock, sortKeyBlock);
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock, LongBlock sortKeyBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int sortKeyValueCount = sortKeyBlock.getValueCount(p);
      if (sortKeyValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        int sortKeyStart = sortKeyBlock.getFirstValueIndex(p);
        int sortKeyEnd = sortKeyStart + sortKeyValueCount;
        for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
          long sortKeyValue = sortKeyBlock.getLong(sortKeyOffset);
          AllFirstExponentialHistogramByLongAggregator.combine(state, valueValue, sortKeyValue);
        }
      }
    }
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock, LongBlock sortKeyBlock,
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
      int sortKeyValueCount = sortKeyBlock.getValueCount(p);
      if (sortKeyValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        int sortKeyStart = sortKeyBlock.getFirstValueIndex(p);
        int sortKeyEnd = sortKeyStart + sortKeyValueCount;
        for (int sortKeyOffset = sortKeyStart; sortKeyOffset < sortKeyEnd; sortKeyOffset++) {
          long sortKeyValue = sortKeyBlock.getLong(sortKeyOffset);
          AllFirstExponentialHistogramByLongAggregator.combine(state, valueValue, sortKeyValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block sortKeysUncast = page.getBlock(channels.get(0));
    LongBlock sortKeys = (LongBlock) sortKeysUncast;
    assert sortKeys.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(1));
    ExponentialHistogramBlock values = (ExponentialHistogramBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(2));
    BooleanBlock seen = (BooleanBlock) seenUncast;
    assert seen.getPositionCount() == 1;
    ExponentialHistogramScratch valuesScratch = new ExponentialHistogramScratch();
    AllFirstExponentialHistogramByLongAggregator.combineIntermediate(state, sortKeys.getLong(0), values, seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = AllFirstExponentialHistogramByLongAggregator.evaluateFinal(state, driverContext);
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
