// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.ArithmeticException;
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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link SumDenseVectorAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SumDenseVectorAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sum", ElementType.FLOAT),
      new IntermediateStateDesc("failed", ElementType.BOOLEAN)  );

  private final Warnings warnings;

  private final DriverContext driverContext;

  private final SumDenseVectorAggregator.SingleState state;

  private final List<ExpressionEvaluator> inputs;

  SumDenseVectorAggregatorFunction(Warnings warnings, DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.warnings = warnings;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SumDenseVectorAggregator.initSingle();
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
    try (Block vectorUncast = inputs.get(0).eval(page)) {
      FloatBlock vectorBlock = (FloatBlock) vectorUncast;
      if (vectorBlock.areAllValuesNull()) {
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
      addRawBlock(vectorBlock, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block vectorUncast = inputs.get(0).eval(page)) {
      FloatBlock vectorBlock = (FloatBlock) vectorUncast;
      if (vectorBlock.areAllValuesNull()) {
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
      addRawBlock(vectorBlock);
    }
  }

  private void addRawBlock(FloatBlock vectorBlock) {
    for (int p = 0; p < vectorBlock.getPositionCount(); p++) {
      try {
        SumDenseVectorAggregator.combine(state, p, vectorBlock);
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.failed(true);
        return;
      }
    }
  }

  private void addRawBlock(FloatBlock vectorBlock, BooleanVector mask) {
    for (int p = 0; p < vectorBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      try {
        SumDenseVectorAggregator.combine(state, p, vectorBlock);
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.failed(true);
        return;
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block sumUncast = inputs.get(0).eval(page);
      Block failedUncast = inputs.get(1).eval(page);
    ) {
      if (sumUncast.areAllValuesNull()) {
        return;
      }
      FloatBlock sum = (FloatBlock) sumUncast;
      assert sum.getPositionCount() == 1;
      if (failedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
      assert failed.getPositionCount() == 1;
      SumDenseVectorAggregator.combineIntermediate(state, sum, failed.getBoolean(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SumDenseVectorAggregator.evaluateFinal(state, driverContext);
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
