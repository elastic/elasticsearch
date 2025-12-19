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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link HistogramMergeTDigestAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class HistogramMergeTDigestAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.TDIGEST),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final TDigestStates.SingleState state;

  private final List<Integer> channels;

  public HistogramMergeTDigestAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, TDigestStates.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static HistogramMergeTDigestAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new HistogramMergeTDigestAggregatorFunction(driverContext, channels, HistogramMergeTDigestAggregator.initSingle(driverContext));
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
    TDigestBlock valueBlock = page.getBlock(channels.get(0));
    addRawBlock(valueBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    TDigestBlock valueBlock = page.getBlock(channels.get(0));
    addRawBlock(valueBlock);
  }

  private void addRawBlock(TDigestBlock valueBlock) {
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset);
        HistogramMergeTDigestAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(TDigestBlock valueBlock, BooleanVector mask) {
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
        TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset);
        HistogramMergeTDigestAggregator.combine(state, valueValue);
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
    TDigestBlock value = (TDigestBlock) valueUncast;
    assert value.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    HistogramMergeTDigestAggregator.combineIntermediate(state, value.getTDigestHolder(value.getFirstValueIndex(0)), seen.getBoolean(0));
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = HistogramMergeTDigestAggregator.evaluateFinal(state, driverContext);
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
