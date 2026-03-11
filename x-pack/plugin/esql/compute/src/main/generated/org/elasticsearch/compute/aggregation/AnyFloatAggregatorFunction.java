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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link AnyFloatAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class AnyFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("observed", ElementType.BOOLEAN),
      new IntermediateStateDesc("values", ElementType.FLOAT)  );

  private final DriverContext driverContext;

  private final AnyFloatAggregator.SingleState state;

  private final List<Integer> channels;

  public AnyFloatAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      AnyFloatAggregator.SingleState state) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static AnyFloatAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels) {
    return new AnyFloatAggregatorFunction(driverContext, channels, AnyFloatAggregator.initSingle(driverContext));
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
    addRawBlock(valuesBlock, mask);
  }

  private void addRawInputNotMasked(Page page) {
    FloatBlock valuesBlock = page.getBlock(channels.get(0));
    addRawBlock(valuesBlock);
  }

  private void addRawBlock(FloatBlock valuesBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      AnyFloatAggregator.combine(state, p, valuesBlock);
    }
  }

  private void addRawBlock(FloatBlock valuesBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      AnyFloatAggregator.combine(state, p, valuesBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block observedUncast = page.getBlock(channels.get(0));
    if (observedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector observed = ((BooleanBlock) observedUncast).asVector();
    assert observed.getPositionCount() == 1;
    Block valuesUncast = page.getBlock(channels.get(1));
    if (valuesUncast.areAllValuesNull()) {
      return;
    }
    FloatBlock values = (FloatBlock) valuesUncast;
    assert values.getPositionCount() == 1;
    AnyFloatAggregator.combineIntermediate(state, observed.getBoolean(0), values);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = AnyFloatAggregator.evaluateFinal(state, driverContext);
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
