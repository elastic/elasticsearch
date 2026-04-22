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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

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

  private final List<ExpressionEvaluator> inputs;

  HistogramMergeTDigestAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = HistogramMergeTDigestAggregator.initSingle(driverContext);
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
    try (Block valueUncast = inputs.get(0).eval(page)) {
      TDigestBlock valueBlock = (TDigestBlock) valueUncast;
      if (valueBlock.areAllValuesNull()) {
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
      addRawBlock(valueBlock, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (Block valueUncast = inputs.get(0).eval(page)) {
      TDigestBlock valueBlock = (TDigestBlock) valueUncast;
      if (valueBlock.areAllValuesNull()) {
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
      addRawBlock(valueBlock);
    }
  }

  private void addRawBlock(TDigestBlock valueBlock) {
    TDigestHolder valueScratch = new TDigestHolder();
    for (int p = 0; p < valueBlock.getPositionCount(); p++) {
      int valueValueCount = valueBlock.getValueCount(p);
      if (valueValueCount == 0) {
        continue;
      }
      int valueStart = valueBlock.getFirstValueIndex(p);
      int valueEnd = valueStart + valueValueCount;
      for (int valueOffset = valueStart; valueOffset < valueEnd; valueOffset++) {
        TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset, valueScratch);
        HistogramMergeTDigestAggregator.combine(state, valueValue);
      }
    }
  }

  private void addRawBlock(TDigestBlock valueBlock, BooleanVector mask) {
    TDigestHolder valueScratch = new TDigestHolder();
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
        TDigestHolder valueValue = valueBlock.getTDigestHolder(valueOffset, valueScratch);
        HistogramMergeTDigestAggregator.combine(state, valueValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block valueUncast = inputs.get(0).eval(page);
      Block seenUncast = inputs.get(1).eval(page);
    ) {
      if (valueUncast.areAllValuesNull()) {
        return;
      }
      TDigestBlock value = (TDigestBlock) valueUncast;
      assert value.getPositionCount() == 1;
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert seen.getPositionCount() == 1;
      TDigestHolder valueScratch = new TDigestHolder();
      HistogramMergeTDigestAggregator.combineIntermediate(state, value.getTDigestHolder(value.getFirstValueIndex(0), valueScratch), seen.getBoolean(0));
    }
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
