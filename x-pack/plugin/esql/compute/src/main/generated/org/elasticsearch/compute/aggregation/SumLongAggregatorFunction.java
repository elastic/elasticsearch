// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.ArithmeticException;
import java.lang.Integer;
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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link AggregatorFunction} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sum", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN),
      new IntermediateStateDesc("failed", ElementType.BOOLEAN)  );

  private final Warnings warnings;

  private final DriverContext driverContext;

  private final LongFallibleState state;

  private final List<Integer> channels;

  public SumLongAggregatorFunction(Warnings warnings, DriverContext driverContext,
      List<Integer> channels, LongFallibleState state) {
    this.warnings = warnings;
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
  }

  public static SumLongAggregatorFunction create(Warnings warnings, DriverContext driverContext,
      List<Integer> channels) {
    return new SumLongAggregatorFunction(warnings, driverContext, channels, new LongFallibleState(SumLongAggregator.init()));
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
    if (state.failed()) {
      return;
    }
    if (mask.allFalse()) {
      // Entire page masked away
      return;
    }
    if (mask.allTrue()) {
      // No masking
      LongBlock block = page.getBlock(channels.get(0));
      LongVector vector = block.asVector();
      if (vector != null) {
        addRawVector(vector);
      } else {
        addRawBlock(block);
      }
      return;
    }
    // Some positions masked away, others kept
    LongBlock block = page.getBlock(channels.get(0));
    LongVector vector = block.asVector();
    if (vector != null) {
      addRawVector(vector, mask);
    } else {
      addRawBlock(block, mask);
    }
  }

  private void addRawVector(LongVector vector) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      try {
        state.longValue(SumLongAggregator.combine(state.longValue(), vector.getLong(i)));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.failed(true);
        return;
      }
    }
  }

  private void addRawVector(LongVector vector, BooleanVector mask) {
    state.seen(true);
    for (int i = 0; i < vector.getPositionCount(); i++) {
      if (mask.getBoolean(i) == false) {
        continue;
      }
      try {
        state.longValue(SumLongAggregator.combine(state.longValue(), vector.getLong(i)));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.failed(true);
        return;
      }
    }
  }

  private void addRawBlock(LongBlock block) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        try {
          state.longValue(SumLongAggregator.combine(state.longValue(), block.getLong(i)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.failed(true);
          return;
        }
      }
    }
  }

  private void addRawBlock(LongBlock block, BooleanVector mask) {
    for (int p = 0; p < block.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      if (block.isNull(p)) {
        continue;
      }
      state.seen(true);
      int start = block.getFirstValueIndex(p);
      int end = start + block.getValueCount(p);
      for (int i = start; i < end; i++) {
        try {
          state.longValue(SumLongAggregator.combine(state.longValue(), block.getLong(i)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.failed(true);
          return;
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block sumUncast = page.getBlock(channels.get(0));
    if (sumUncast.areAllValuesNull()) {
      return;
    }
    LongVector sum = ((LongBlock) sumUncast).asVector();
    assert sum.getPositionCount() == 1;
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    assert seen.getPositionCount() == 1;
    Block failedUncast = page.getBlock(channels.get(2));
    if (failedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
    assert failed.getPositionCount() == 1;
    if (failed.getBoolean(0)) {
      state.failed(true);
      state.seen(true);
    } else if (seen.getBoolean(0)) {
      try {
        state.longValue(SumLongAggregator.combine(state.longValue(), sum.getLong(0)));
        state.seen(true);
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.failed(true);
      }
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    if (state.seen() == false || state.failed()) {
      blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
      return;
    }
    blocks[offset] = driverContext.blockFactory().newConstantLongBlockWith(state.longValue(), 1);
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
