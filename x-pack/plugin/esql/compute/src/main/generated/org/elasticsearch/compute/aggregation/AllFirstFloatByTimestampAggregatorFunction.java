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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link AllFirstFloatByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class AllFirstFloatByTimestampAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("observed", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestampPresent", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestamp", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.FLOAT)  );

  private final DriverContext driverContext;

  private final AllLongFloatState state;

  private final List<Integer> channels;

  public AllFirstFloatByTimestampAggregatorFunction(DriverContext driverContext,
      List<Integer> channels, AllLongFloatState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static AllFirstFloatByTimestampAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new AllFirstFloatByTimestampAggregatorFunction(driverContext, channels, AllFirstFloatByTimestampAggregator.initSingle(driverContext));
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
    FloatBlock valuesBlock = page.getBlock(channels.get(0));
    LongBlock timestampsBlock = page.getBlock(channels.get(1));
    addRawBlock(valuesBlock, timestampsBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    FloatBlock valuesBlock = page.getBlock(channels.get(0));
    LongBlock timestampsBlock = page.getBlock(channels.get(1));
    addRawBlock(valuesBlock, timestampsBlock);
  }

  private void addRawBlock(FloatBlock valuesBlock, LongBlock timestampsBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      AllFirstFloatByTimestampAggregator.combine(state, p, valuesBlock, timestampsBlock);
    }
  }

  private void addRawBlock(FloatBlock valuesBlock, LongBlock timestampsBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      AllFirstFloatByTimestampAggregator.combine(state, p, valuesBlock, timestampsBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block observedUncast = page.getBlock(channels.get(0));
    BooleanBlock observed = (BooleanBlock) observedUncast;
    assert observed.getPositionCount() == 1;
    Block timestampPresentUncast = page.getBlock(channels.get(1));
    BooleanBlock timestampPresent = (BooleanBlock) timestampPresentUncast;
    assert timestampPresent.getPositionCount() == 1;
    Block timestampUncast = page.getBlock(channels.get(2));
    LongBlock timestamp = (LongBlock) timestampUncast;
    assert timestamp.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(3));
    FloatBlock values = (FloatBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    AllFirstFloatByTimestampAggregator.combineIntermediate(state, observed.getBoolean(0), timestampPresent.getBoolean(0), timestamp.getLong(0), values);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = AllFirstFloatByTimestampAggregator.evaluateFinal(state, driverContext);
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
