// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link AllLastIntByLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class AllLastIntByLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("observed", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestampPresent", ElementType.BOOLEAN),
      new IntermediateStateDesc("timestamp", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.INT)  );

  private final DriverContext driverContext;

  private final AllLongIntState state;

  private final List<ExpressionEvaluator> inputs;

  AllLastIntByLongAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = AllLastIntByLongAggregator.initSingle(driverContext);
    boolean success = false;
    try {
      driverContext.breaker().addEstimateBytesAndMaybeBreak(ExpressionEvaluator.totalRamBytesUsed(inputs), "ESQL");
      success = true;
    } finally {
      if (success == false) {
        this.state.close();
      }
    }
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
    try (
      Block valuesUncast = inputs.get(0).eval(page);
      Block timestampsUncast = inputs.get(1).eval(page);
    ) {
      IntBlock valuesBlock = (IntBlock) valuesUncast;
      LongBlock timestampsBlock = (LongBlock) timestampsUncast;
      addRawBlock(valuesBlock, timestampsBlock, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (
      Block valuesUncast = inputs.get(0).eval(page);
      Block timestampsUncast = inputs.get(1).eval(page);
    ) {
      IntBlock valuesBlock = (IntBlock) valuesUncast;
      LongBlock timestampsBlock = (LongBlock) timestampsUncast;
      addRawBlock(valuesBlock, timestampsBlock);
    }
  }

  private void addRawBlock(IntBlock valuesBlock, LongBlock timestampsBlock) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      AllLastIntByLongAggregator.combine(state, p, valuesBlock, timestampsBlock);
    }
  }

  private void addRawBlock(IntBlock valuesBlock, LongBlock timestampsBlock, BooleanVector mask) {
    for (int p = 0; p < valuesBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      AllLastIntByLongAggregator.combine(state, p, valuesBlock, timestampsBlock);
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block observedUncast = inputs.get(0).eval(page);
      Block timestampPresentUncast = inputs.get(1).eval(page);
      Block timestampUncast = inputs.get(2).eval(page);
      Block valuesUncast = inputs.get(3).eval(page);
    ) {
      BooleanBlock observed = (BooleanBlock) observedUncast;
      assert observed.getPositionCount() == 1;
      BooleanBlock timestampPresent = (BooleanBlock) timestampPresentUncast;
      assert timestampPresent.getPositionCount() == 1;
      LongBlock timestamp = (LongBlock) timestampUncast;
      assert timestamp.getPositionCount() == 1;
      IntBlock values = (IntBlock) valuesUncast;
      assert values.getPositionCount() == 1;
      AllLastIntByLongAggregator.combineIntermediate(state, observed.getBoolean(0), timestampPresent.getBoolean(0), timestamp.getLong(0), values);
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = AllLastIntByLongAggregator.evaluateFinal(state, driverContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("inputs=").append(inputs);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(
          state,
          Releasables.wrap(inputs),
          () -> driverContext.breaker().addWithoutBreaking(-ExpressionEvaluator.totalRamBytesUsed(inputs))
        );
  }
}
