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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * {@link AggregatorFunction} implementation for {@link FirstTDigestByTimestampAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class FirstTDigestByTimestampAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("values", ElementType.TDIGEST),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final DriverContext driverContext;

  private final TDigestStates.WithLongSingleState state;

  private final List<ExpressionEvaluator> inputs;

  FirstTDigestByTimestampAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = FirstTDigestByTimestampAggregator.initSingle(driverContext);
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
      Block tdigestUncast = inputs.get(0).eval(page);
      Block timestampUncast = inputs.get(1).eval(page);
    ) {
      TDigestBlock tdigestBlock = (TDigestBlock) tdigestUncast;
      LongBlock timestampBlock = (LongBlock) timestampUncast;
      addRawBlock(tdigestBlock, timestampBlock, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (
      Block tdigestUncast = inputs.get(0).eval(page);
      Block timestampUncast = inputs.get(1).eval(page);
    ) {
      TDigestBlock tdigestBlock = (TDigestBlock) tdigestUncast;
      LongBlock timestampBlock = (LongBlock) timestampUncast;
      addRawBlock(tdigestBlock, timestampBlock);
    }
  }

  private void addRawBlock(TDigestBlock tdigestBlock, LongBlock timestampBlock) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int p = 0; p < tdigestBlock.getPositionCount(); p++) {
      int tdigestValueCount = tdigestBlock.getValueCount(p);
      if (tdigestValueCount == 0) {
        continue;
      }
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int tdigestStart = tdigestBlock.getFirstValueIndex(p);
      int tdigestEnd = tdigestStart + tdigestValueCount;
      for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
        TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          FirstTDigestByTimestampAggregator.combine(state, tdigestValue, timestampValue);
        }
      }
    }
  }

  private void addRawBlock(TDigestBlock tdigestBlock, LongBlock timestampBlock,
      BooleanVector mask) {
    TDigestHolder tdigestScratch = new TDigestHolder();
    for (int p = 0; p < tdigestBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int tdigestValueCount = tdigestBlock.getValueCount(p);
      if (tdigestValueCount == 0) {
        continue;
      }
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int tdigestStart = tdigestBlock.getFirstValueIndex(p);
      int tdigestEnd = tdigestStart + tdigestValueCount;
      for (int tdigestOffset = tdigestStart; tdigestOffset < tdigestEnd; tdigestOffset++) {
        TDigestHolder tdigestValue = tdigestBlock.getTDigestHolder(tdigestOffset, tdigestScratch);
        int timestampStart = timestampBlock.getFirstValueIndex(p);
        int timestampEnd = timestampStart + timestampValueCount;
        for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
          long timestampValue = timestampBlock.getLong(timestampOffset);
          FirstTDigestByTimestampAggregator.combine(state, tdigestValue, timestampValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (
      Block timestampsUncast = inputs.get(0).eval(page);
      Block valuesUncast = inputs.get(1).eval(page);
      Block seenUncast = inputs.get(2).eval(page);
    ) {
      if (timestampsUncast.areAllValuesNull()) {
        return;
      }
      LongVector timestamps = ((LongBlock) timestampsUncast).asVector();
      assert timestamps.getPositionCount() == 1;
      if (valuesUncast.areAllValuesNull()) {
        return;
      }
      TDigestBlock values = (TDigestBlock) valuesUncast;
      assert values.getPositionCount() == 1;
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert seen.getPositionCount() == 1;
      TDigestHolder valuesScratch = new TDigestHolder();
      FirstTDigestByTimestampAggregator.combineIntermediate(state, timestamps.getLong(0), values.getTDigestHolder(values.getFirstValueIndex(0), valuesScratch), seen.getBoolean(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = FirstTDigestByTimestampAggregator.evaluateFinal(state, driverContext);
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
