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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link LossySumDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class LossySumDoubleAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("value", ElementType.DOUBLE),
      new IntermediateStateDesc("unusedDeltas", ElementType.DOUBLE),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final LossySumDoubleAggregator.SumState state;

  private final List<ExpressionEvaluator> inputs;

  LossySumDoubleAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = LossySumDoubleAggregator.initSingle();
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
    try (Block vUncast = inputs.get(0).eval(page)) {
      DoubleBlock vBlock = (DoubleBlock) vUncast;
      DoubleVector vVector = vBlock.asVector();
      if (vVector == null) {
        if (vBlock.areAllValuesNull()) {
          /*
           * All values are null so we can skip processing this block.
           * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
           *       being fast without this. Likely the branch predictor is kicking
           *       in there. But we do this anyway, just so we don't have to trust
           *       it. It's magic. Glorious magic. But it's deep magic. And we won't
           *       always have long sequences of ConstantNullBlock. And this code
           *       shows readers we've thought about this.
           */
          return;
        }
        addRawBlock(vBlock, mask);
        return;
      }
      addRawVector(vVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block vUncast = inputs.get(0).eval(page)) {
      DoubleBlock vBlock = (DoubleBlock) vUncast;
      DoubleVector vVector = vBlock.asVector();
      if (vVector == null) {
        if (vBlock.areAllValuesNull()) {
          /*
           * All values are null so we can skip processing this block.
           * NOTE: Microbenchmarks point to long sequences of ConstantNullBlocks
           *       being fast without this. Likely the branch predictor is kicking
           *       in there. But we do this anyway, just so we don't have to trust
           *       it. It's magic. Glorious magic. But it's deep magic. And we won't
           *       always have long sequences of ConstantNullBlock. And this code
           *       shows readers we've thought about this.
           */
          return;
        }
        addRawBlock(vBlock);
        return;
      }
      addRawVector(vVector);
    }
  }

  private void addRawVector(DoubleVector vVector) {
    state.seen(true);
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      double vValue = vVector.getDouble(valuesPosition);
      LossySumDoubleAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(DoubleVector vVector, BooleanVector mask) {
    state.seen(true);
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      double vValue = vVector.getDouble(valuesPosition);
      LossySumDoubleAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(DoubleBlock vBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      state.seen(true);
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        double vValue = vBlock.getDouble(vOffset);
        LossySumDoubleAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(DoubleBlock vBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      state.seen(true);
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        double vValue = vBlock.getDouble(vOffset);
        LossySumDoubleAggregator.combine(state, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block valueUncast = inputs.get(0).eval(page);
      Block unusedDeltasUncast = inputs.get(1).eval(page);
      Block seenUncast = inputs.get(2).eval(page);
    ) {
      if (valueUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector value = ((DoubleBlock) valueUncast).asVector();
      assert value.getPositionCount() == 1;
      if (unusedDeltasUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector unusedDeltas = ((DoubleBlock) unusedDeltasUncast).asVector();
      assert unusedDeltas.getPositionCount() == 1;
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert seen.getPositionCount() == 1;
      LossySumDoubleAggregator.combineIntermediate(state, value.getDouble(0), unusedDeltas.getDouble(0), seen.getBoolean(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    if (state.seen() == false) {
      blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
      return;
    }
    blocks[offset] = LossySumDoubleAggregator.evaluateFinal(state, driverContext);
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
