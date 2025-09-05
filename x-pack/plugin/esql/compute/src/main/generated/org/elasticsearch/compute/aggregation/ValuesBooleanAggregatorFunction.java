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
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link ValuesBooleanAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class ValuesBooleanAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("values", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final ValuesBooleanAggregator.SingleState state;

  private final List<Integer> channels;

  public ValuesBooleanAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      ValuesBooleanAggregator.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static ValuesBooleanAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new ValuesBooleanAggregatorFunction(driverContext, channels, ValuesBooleanAggregator.initSingle());
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
    BooleanBlock vBlock = page.getBlock(channels.get(0));
    BooleanVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, mask);
      return;
    }
    addRawVector(vVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    BooleanBlock vBlock = page.getBlock(channels.get(0));
    BooleanVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock);
      return;
    }
    addRawVector(vVector);
  }

  private void addRawVector(BooleanVector vVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      boolean vValue = vVector.getBoolean(valuesPosition);
      ValuesBooleanAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(BooleanVector vVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      boolean vValue = vVector.getBoolean(valuesPosition);
      ValuesBooleanAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(BooleanBlock vBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (vBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        boolean vValue = vBlock.getBoolean(vOffset);
        ValuesBooleanAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(BooleanBlock vBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (vBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        boolean vValue = vBlock.getBoolean(vOffset);
        ValuesBooleanAggregator.combine(state, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block valuesUncast = page.getBlock(channels.get(0));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    BooleanBlock values = (BooleanBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    ValuesBooleanAggregator.combineIntermediate(state, values);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = ValuesBooleanAggregator.evaluateFinal(state, driverContext);
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
