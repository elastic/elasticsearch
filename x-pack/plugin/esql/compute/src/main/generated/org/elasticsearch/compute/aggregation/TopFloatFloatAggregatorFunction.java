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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link TopFloatFloatAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopFloatFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.FLOAT),
      new IntermediateStateDesc("extra", ElementType.FLOAT)  );

  private final DriverContext driverContext;

  private final TopFloatFloatAggregator.SingleState state;

  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopFloatFloatAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      TopFloatFloatAggregator.SingleState state, int limit, boolean ascending) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopFloatFloatAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int limit, boolean ascending) {
    return new TopFloatFloatAggregatorFunction(driverContext, channels, TopFloatFloatAggregator.initSingle(driverContext.bigArrays(), limit, ascending), limit, ascending);
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
    FloatBlock vBlock = page.getBlock(channels.get(0));
    FloatBlock extraBlock = page.getBlock(channels.get(1));
    FloatVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, extraBlock, mask);
      return;
    }
    FloatVector extraVector = extraBlock.asVector();
    if (extraVector == null) {
      addRawBlock(vBlock, extraBlock, mask);
      return;
    }
    addRawVector(vVector, extraVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    FloatBlock vBlock = page.getBlock(channels.get(0));
    FloatBlock extraBlock = page.getBlock(channels.get(1));
    FloatVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, extraBlock);
      return;
    }
    FloatVector extraVector = extraBlock.asVector();
    if (extraVector == null) {
      addRawBlock(vBlock, extraBlock);
      return;
    }
    addRawVector(vVector, extraVector);
  }

  private void addRawVector(FloatVector vVector, FloatVector extraVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      float vValue = vVector.getFloat(valuesPosition);
      float extraValue = extraVector.getFloat(valuesPosition);
      TopFloatFloatAggregator.combine(state, vValue, extraValue);
    }
  }

  private void addRawVector(FloatVector vVector, FloatVector extraVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      float vValue = vVector.getFloat(valuesPosition);
      float extraValue = extraVector.getFloat(valuesPosition);
      TopFloatFloatAggregator.combine(state, vValue, extraValue);
    }
  }

  private void addRawBlock(FloatBlock vBlock, FloatBlock extraBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (vBlock.isNull(p)) {
        continue;
      }
      if (extraBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        int extraStart = extraBlock.getFirstValueIndex(p);
        int extraEnd = extraStart + extraBlock.getValueCount(p);
        for (int extraOffset = extraStart; extraOffset < extraEnd; extraOffset++) {
          float extraValue = extraBlock.getFloat(extraOffset);
          TopFloatFloatAggregator.combine(state, vValue, extraValue);
        }
      }
    }
  }

  private void addRawBlock(FloatBlock vBlock, FloatBlock extraBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (vBlock.isNull(p)) {
        continue;
      }
      if (extraBlock.isNull(p)) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vBlock.getValueCount(p);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        int extraStart = extraBlock.getFirstValueIndex(p);
        int extraEnd = extraStart + extraBlock.getValueCount(p);
        for (int extraOffset = extraStart; extraOffset < extraEnd; extraOffset++) {
          float extraValue = extraBlock.getFloat(extraOffset);
          TopFloatFloatAggregator.combine(state, vValue, extraValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock top = (FloatBlock) topUncast;
    assert top.getPositionCount() == 1;
    Block extraUncast = page.getBlock(channels.get(1));
    if (extraUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock extra = (FloatBlock) extraUncast;
    assert extra.getPositionCount() == 1;
    TopFloatFloatAggregator.combineIntermediate(state, top, extra);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopFloatFloatAggregator.evaluateFinal(state, driverContext);
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
