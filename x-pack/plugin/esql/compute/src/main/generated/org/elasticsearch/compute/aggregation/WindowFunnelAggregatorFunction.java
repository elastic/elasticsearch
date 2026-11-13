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
import org.elasticsearch.compute.data.ConstantLongVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.LongArrowBufVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link WindowFunnelAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class WindowFunnelAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("timestamps", ElementType.LONG),
      new IntermediateStateDesc("masks", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final WindowFunnelAggregator.SingleState state;

  private final List<Integer> channels;

  private final long span;

  private final int steps;

  WindowFunnelAggregatorFunction(DriverContext driverContext, List<Integer> channels, long span,
      int steps) {
    this.span = span;
    this.steps = steps;
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = WindowFunnelAggregator.initSingle(driverContext, span, steps);
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
    LongBlock timestampBlock = page.getBlock(channels.get(0));
    LongBlock maskBlock = page.getBlock(channels.get(1));
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      if (timestampBlock.areAllValuesNull()) {
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
      addRawBlock(timestampBlock, maskBlock, mask);
      return;
    }
    LongVector maskVector = maskBlock.asVector();
    if (maskVector == null) {
      if (maskBlock.areAllValuesNull()) {
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
      addRawBlock(timestampBlock, maskBlock, mask);
      return;
    }
    addRawVector(timestampVector, maskVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    LongBlock timestampBlock = page.getBlock(channels.get(0));
    LongBlock maskBlock = page.getBlock(channels.get(1));
    LongVector timestampVector = timestampBlock.asVector();
    if (timestampVector == null) {
      if (timestampBlock.areAllValuesNull()) {
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
      addRawBlock(timestampBlock, maskBlock);
      return;
    }
    LongVector maskVector = maskBlock.asVector();
    if (maskVector == null) {
      if (maskBlock.areAllValuesNull()) {
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
      addRawBlock(timestampBlock, maskBlock);
      return;
    }
    addRawVector(timestampVector, maskVector);
  }

  private void addRawVector(LongVector timestampVector, LongVector maskVector) {
    if (timestampVector.getClass() == LongArrayVector.class) {
      LongArrayVector specialized = (LongArrayVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    if (timestampVector.getClass() == LongArrowBufVector.class) {
      LongArrowBufVector specialized = (LongArrowBufVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    if (timestampVector.getClass() == ConstantLongVector.class) {
      ConstantLongVector specialized = (ConstantLongVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    for (int valuesPosition = 0; valuesPosition < timestampVector.getPositionCount(); valuesPosition++) {
      long timestampValue = timestampVector.getLong(valuesPosition);
      long maskValue = maskVector.getLong(valuesPosition);
      WindowFunnelAggregator.combine(state, timestampValue, maskValue);
    }
  }

  private void addRawVector(LongVector timestampVector, LongVector maskVector, BooleanVector mask) {
    if (timestampVector.getClass() == LongArrayVector.class) {
      LongArrayVector specialized = (LongArrayVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        if (mask.getBoolean(valuesPosition) == false) {
          continue;
        }
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    if (timestampVector.getClass() == LongArrowBufVector.class) {
      LongArrowBufVector specialized = (LongArrowBufVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        if (mask.getBoolean(valuesPosition) == false) {
          continue;
        }
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    if (timestampVector.getClass() == ConstantLongVector.class) {
      ConstantLongVector specialized = (ConstantLongVector) timestampVector;
      for (int valuesPosition = 0; valuesPosition < specialized.getPositionCount(); valuesPosition++) {
        if (mask.getBoolean(valuesPosition) == false) {
          continue;
        }
        long timestampValue = specialized.getLong(valuesPosition);
        long maskValue = maskVector.getLong(valuesPosition);
        WindowFunnelAggregator.combine(state, timestampValue, maskValue);
      }
      return;
    }
    for (int valuesPosition = 0; valuesPosition < timestampVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long timestampValue = timestampVector.getLong(valuesPosition);
      long maskValue = maskVector.getLong(valuesPosition);
      WindowFunnelAggregator.combine(state, timestampValue, maskValue);
    }
  }

  private void addRawBlock(LongBlock timestampBlock, LongBlock maskBlock) {
    for (int p = 0; p < timestampBlock.getPositionCount(); p++) {
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int maskValueCount = maskBlock.getValueCount(p);
      if (maskValueCount == 0) {
        continue;
      }
      int timestampStart = timestampBlock.getFirstValueIndex(p);
      int timestampEnd = timestampStart + timestampValueCount;
      for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
        long timestampValue = timestampBlock.getLong(timestampOffset);
        int maskStart = maskBlock.getFirstValueIndex(p);
        int maskEnd = maskStart + maskValueCount;
        for (int maskOffset = maskStart; maskOffset < maskEnd; maskOffset++) {
          long maskValue = maskBlock.getLong(maskOffset);
          WindowFunnelAggregator.combine(state, timestampValue, maskValue);
        }
      }
    }
  }

  private void addRawBlock(LongBlock timestampBlock, LongBlock maskBlock, BooleanVector mask) {
    for (int p = 0; p < timestampBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int timestampValueCount = timestampBlock.getValueCount(p);
      if (timestampValueCount == 0) {
        continue;
      }
      int maskValueCount = maskBlock.getValueCount(p);
      if (maskValueCount == 0) {
        continue;
      }
      int timestampStart = timestampBlock.getFirstValueIndex(p);
      int timestampEnd = timestampStart + timestampValueCount;
      for (int timestampOffset = timestampStart; timestampOffset < timestampEnd; timestampOffset++) {
        long timestampValue = timestampBlock.getLong(timestampOffset);
        int maskStart = maskBlock.getFirstValueIndex(p);
        int maskEnd = maskStart + maskValueCount;
        for (int maskOffset = maskStart; maskOffset < maskEnd; maskOffset++) {
          long maskValue = maskBlock.getLong(maskOffset);
          WindowFunnelAggregator.combine(state, timestampValue, maskValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block timestampsUncast = page.getBlock(channels.get(0));
    if (timestampsUncast.areAllValuesNull()) {
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
    LongBlock timestamps = (LongBlock) timestampsUncast;
    assert timestamps.getPositionCount() == 1;
    Block masksUncast = page.getBlock(channels.get(1));
    if (masksUncast.areAllValuesNull()) {
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
    LongBlock masks = (LongBlock) masksUncast;
    assert masks.getPositionCount() == 1;
    WindowFunnelAggregator.combineIntermediate(state, timestamps, masks);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = WindowFunnelAggregator.evaluateFinal(state, driverContext);
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
