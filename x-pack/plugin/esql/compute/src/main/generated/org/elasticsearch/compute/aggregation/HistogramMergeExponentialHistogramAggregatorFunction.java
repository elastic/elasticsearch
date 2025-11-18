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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * {@link AggregatorFunction} implementation for {@link HistogramMergeExponentialHistogramAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class HistogramMergeExponentialHistogramAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.EXPONENTIAL_HISTOGRAM)  );

  private final DriverContext driverContext;

  private final ExponentialHistogramStates.SingleState state;

  private final List<Integer> channels;

  public HistogramMergeExponentialHistogramAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, ExponentialHistogramStates.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static HistogramMergeExponentialHistogramAggregatorFunction create(
      DriverContext driverContext, List<Integer> channels) {
    return new HistogramMergeExponentialHistogramAggregatorFunction(driverContext, channels, HistogramMergeExponentialHistogramAggregator.initSingle(driverContext));
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
    addRawBlock(valueBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    ExponentialHistogramBlock valueBlock = page.getBlock(channels.get(0));
    addRawBlock(valueBlock);
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        HistogramMergeExponentialHistogramAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(ExponentialHistogramBlock valueBlock, BooleanVector mask) {
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
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
        ExponentialHistogram valueValue = valueBlock.getExponentialHistogram(valueOffset, valueScratch);
        HistogramMergeExponentialHistogramAggregator.combine(state, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block valueUncast = page.getBlock(channels.get(0));
    if (valueUncast.areAllValuesNull()) {
      return;
    }
    ExponentialHistogramBlock value = (ExponentialHistogramBlock) valueUncast;
    assert value.getPositionCount() == 1;
    ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
    HistogramMergeExponentialHistogramAggregator.combineIntermediate(state, value.getExponentialHistogram(value.getFirstValueIndex(0), valueScratch));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = HistogramMergeExponentialHistogramAggregator.evaluateFinal(state, driverContext);
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
