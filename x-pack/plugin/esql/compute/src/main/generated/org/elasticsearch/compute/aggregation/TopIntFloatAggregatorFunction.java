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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link TopIntFloatAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopIntFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.INT),
      new IntermediateStateDesc("output", ElementType.FLOAT)  );

  private final DriverContext driverContext;

  private final TopIntFloatAggregator.SingleState state;

  private final List<Integer> channels;

  private final int limit;

  private final boolean ascending;

  public TopIntFloatAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      TopIntFloatAggregator.SingleState state, int limit, boolean ascending) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopIntFloatAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, int limit, boolean ascending) {
    return new TopIntFloatAggregatorFunction(driverContext, channels, TopIntFloatAggregator.initSingle(driverContext.bigArrays(), limit, ascending), limit, ascending);
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
    IntBlock vBlock = page.getBlock(channels.get(0));
    FloatBlock outputValueBlock = page.getBlock(channels.get(1));
    IntVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, outputValueBlock, mask);
      return;
    }
    FloatVector outputValueVector = outputValueBlock.asVector();
    if (outputValueVector == null) {
      addRawBlock(vBlock, outputValueBlock, mask);
      return;
    }
    addRawVector(vVector, outputValueVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    IntBlock vBlock = page.getBlock(channels.get(0));
    FloatBlock outputValueBlock = page.getBlock(channels.get(1));
    IntVector vVector = vBlock.asVector();
    if (vVector == null) {
      addRawBlock(vBlock, outputValueBlock);
      return;
    }
    FloatVector outputValueVector = outputValueBlock.asVector();
    if (outputValueVector == null) {
      addRawBlock(vBlock, outputValueBlock);
      return;
    }
    addRawVector(vVector, outputValueVector);
  }

  private void addRawVector(IntVector vVector, FloatVector outputValueVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      int vValue = vVector.getInt(valuesPosition);
      float outputValueValue = outputValueVector.getFloat(valuesPosition);
      TopIntFloatAggregator.combine(state, vValue, outputValueValue);
    }
  }

  private void addRawVector(IntVector vVector, FloatVector outputValueVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      int vValue = vVector.getInt(valuesPosition);
      float outputValueValue = outputValueVector.getFloat(valuesPosition);
      TopIntFloatAggregator.combine(state, vValue, outputValueValue);
    }
  }

  private void addRawBlock(IntBlock vBlock, FloatBlock outputValueBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int outputValueValueCount = outputValueBlock.getValueCount(p);
      if (outputValueValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        int vValue = vBlock.getInt(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(p);
        int outputValueEnd = outputValueStart + outputValueValueCount;
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          float outputValueValue = outputValueBlock.getFloat(outputValueOffset);
          TopIntFloatAggregator.combine(state, vValue, outputValueValue);
        }
      }
    }
  }

  private void addRawBlock(IntBlock vBlock, FloatBlock outputValueBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int outputValueValueCount = outputValueBlock.getValueCount(p);
      if (outputValueValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        int vValue = vBlock.getInt(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(p);
        int outputValueEnd = outputValueStart + outputValueValueCount;
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          float outputValueValue = outputValueBlock.getFloat(outputValueOffset);
          TopIntFloatAggregator.combine(state, vValue, outputValueValue);
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
    IntBlock top = (IntBlock) topUncast;
    assert top.getPositionCount() == 1;
    Block outputUncast = page.getBlock(channels.get(1));
    if (outputUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock output = (FloatBlock) outputUncast;
    assert output.getPositionCount() == 1;
    TopIntFloatAggregator.combineIntermediate(state, top, output);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopIntFloatAggregator.evaluateFinal(state, driverContext);
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
