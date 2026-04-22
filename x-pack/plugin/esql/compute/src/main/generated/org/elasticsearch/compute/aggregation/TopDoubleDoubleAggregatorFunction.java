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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link TopDoubleDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopDoubleDoubleAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.DOUBLE),
      new IntermediateStateDesc("output", ElementType.DOUBLE)  );

  private final DriverContext driverContext;

  private final TopDoubleDoubleAggregator.SingleState state;

  private final List<ExpressionEvaluator> inputs;

  private final int limit;

  private final boolean ascending;

  TopDoubleDoubleAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs,
      int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = TopDoubleDoubleAggregator.initSingle(driverContext.bigArrays(), limit, ascending);
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
      Block vUncast = inputs.get(0).eval(page);
      Block outputValueUncast = inputs.get(1).eval(page);
    ) {
      DoubleBlock vBlock = (DoubleBlock) vUncast;
      DoubleBlock outputValueBlock = (DoubleBlock) outputValueUncast;
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
        addRawBlock(vBlock, outputValueBlock, mask);
        return;
      }
      DoubleVector outputValueVector = outputValueBlock.asVector();
      if (outputValueVector == null) {
        if (outputValueBlock.areAllValuesNull()) {
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
        addRawBlock(vBlock, outputValueBlock, mask);
        return;
      }
      addRawVector(vVector, outputValueVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (
      Block vUncast = inputs.get(0).eval(page);
      Block outputValueUncast = inputs.get(1).eval(page);
    ) {
      DoubleBlock vBlock = (DoubleBlock) vUncast;
      DoubleBlock outputValueBlock = (DoubleBlock) outputValueUncast;
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
        addRawBlock(vBlock, outputValueBlock);
        return;
      }
      DoubleVector outputValueVector = outputValueBlock.asVector();
      if (outputValueVector == null) {
        if (outputValueBlock.areAllValuesNull()) {
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
        addRawBlock(vBlock, outputValueBlock);
        return;
      }
      addRawVector(vVector, outputValueVector);
    }
  }

  private void addRawVector(DoubleVector vVector, DoubleVector outputValueVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      double vValue = vVector.getDouble(valuesPosition);
      double outputValueValue = outputValueVector.getDouble(valuesPosition);
      TopDoubleDoubleAggregator.combine(state, vValue, outputValueValue);
    }
  }

  private void addRawVector(DoubleVector vVector, DoubleVector outputValueVector,
      BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      double vValue = vVector.getDouble(valuesPosition);
      double outputValueValue = outputValueVector.getDouble(valuesPosition);
      TopDoubleDoubleAggregator.combine(state, vValue, outputValueValue);
    }
  }

  private void addRawBlock(DoubleBlock vBlock, DoubleBlock outputValueBlock) {
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
        double vValue = vBlock.getDouble(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(p);
        int outputValueEnd = outputValueStart + outputValueValueCount;
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          double outputValueValue = outputValueBlock.getDouble(outputValueOffset);
          TopDoubleDoubleAggregator.combine(state, vValue, outputValueValue);
        }
      }
    }
  }

  private void addRawBlock(DoubleBlock vBlock, DoubleBlock outputValueBlock, BooleanVector mask) {
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
        double vValue = vBlock.getDouble(vOffset);
        int outputValueStart = outputValueBlock.getFirstValueIndex(p);
        int outputValueEnd = outputValueStart + outputValueValueCount;
        for (int outputValueOffset = outputValueStart; outputValueOffset < outputValueEnd; outputValueOffset++) {
          double outputValueValue = outputValueBlock.getDouble(outputValueOffset);
          TopDoubleDoubleAggregator.combine(state, vValue, outputValueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block topUncast = inputs.get(0).eval(page);
      Block outputUncast = inputs.get(1).eval(page);
    ) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      DoubleBlock top = (DoubleBlock) topUncast;
      assert top.getPositionCount() == 1;
      if (outputUncast.areAllValuesNull()) {
        return;
      }
      DoubleBlock output = (DoubleBlock) outputUncast;
      assert output.getPositionCount() == 1;
      TopDoubleDoubleAggregator.combineIntermediate(state, top, output);
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopDoubleDoubleAggregator.evaluateFinal(state, driverContext);
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
