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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link AggregatorFunction} implementation for {@link PromqlHistogramQuantileAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class PromqlHistogramQuantileAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("buckets", ElementType.DOUBLE)  );

  private final Warnings warnings;

  private final DriverContext driverContext;

  private final PromqlHistogramQuantileStates.SingleState state;

  private final List<Integer> channels;

  private final double quantile;

  PromqlHistogramQuantileAggregatorFunction(Warnings warnings, DriverContext driverContext,
      List<Integer> channels, double quantile) {
    this.quantile = quantile;
    this.warnings = warnings;
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = PromqlHistogramQuantileAggregator.initSingle(driverContext, quantile, warnings);
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
    DoubleBlock countBlock = page.getBlock(channels.get(0));
    BytesRefBlock upperBoundBlock = page.getBlock(channels.get(1));
    DoubleVector countVector = countBlock.asVector();
    if (countVector == null) {
      if (countBlock.areAllValuesNull()) {
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
      addRawBlock(countBlock, upperBoundBlock, mask);
      return;
    }
    BytesRefVector upperBoundVector = upperBoundBlock.asVector();
    if (upperBoundVector == null) {
      if (upperBoundBlock.areAllValuesNull()) {
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
      addRawBlock(countBlock, upperBoundBlock, mask);
      return;
    }
    addRawVector(countVector, upperBoundVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    DoubleBlock countBlock = page.getBlock(channels.get(0));
    BytesRefBlock upperBoundBlock = page.getBlock(channels.get(1));
    DoubleVector countVector = countBlock.asVector();
    if (countVector == null) {
      if (countBlock.areAllValuesNull()) {
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
      addRawBlock(countBlock, upperBoundBlock);
      return;
    }
    BytesRefVector upperBoundVector = upperBoundBlock.asVector();
    if (upperBoundVector == null) {
      if (upperBoundBlock.areAllValuesNull()) {
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
      addRawBlock(countBlock, upperBoundBlock);
      return;
    }
    addRawVector(countVector, upperBoundVector);
  }

  private void addRawVector(DoubleVector countVector, BytesRefVector upperBoundVector) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < countVector.getPositionCount(); valuesPosition++) {
      double countValue = countVector.getDouble(valuesPosition);
      BytesRef upperBoundValue = upperBoundVector.getBytesRef(valuesPosition, upperBoundScratch);
      PromqlHistogramQuantileAggregator.combine(state, countValue, upperBoundValue);
    }
  }

  private void addRawVector(DoubleVector countVector, BytesRefVector upperBoundVector,
      BooleanVector mask) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < countVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      double countValue = countVector.getDouble(valuesPosition);
      BytesRef upperBoundValue = upperBoundVector.getBytesRef(valuesPosition, upperBoundScratch);
      PromqlHistogramQuantileAggregator.combine(state, countValue, upperBoundValue);
    }
  }

  private void addRawBlock(DoubleBlock countBlock, BytesRefBlock upperBoundBlock) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int p = 0; p < countBlock.getPositionCount(); p++) {
      int countValueCount = countBlock.getValueCount(p);
      if (countValueCount == 0) {
        continue;
      }
      int upperBoundValueCount = upperBoundBlock.getValueCount(p);
      if (upperBoundValueCount == 0) {
        continue;
      }
      int countStart = countBlock.getFirstValueIndex(p);
      int countEnd = countStart + countValueCount;
      for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
        double countValue = countBlock.getDouble(countOffset);
        int upperBoundStart = upperBoundBlock.getFirstValueIndex(p);
        int upperBoundEnd = upperBoundStart + upperBoundValueCount;
        for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
          BytesRef upperBoundValue = upperBoundBlock.getBytesRef(upperBoundOffset, upperBoundScratch);
          PromqlHistogramQuantileAggregator.combine(state, countValue, upperBoundValue);
        }
      }
    }
  }

  private void addRawBlock(DoubleBlock countBlock, BytesRefBlock upperBoundBlock,
      BooleanVector mask) {
    BytesRef upperBoundScratch = new BytesRef();
    for (int p = 0; p < countBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int countValueCount = countBlock.getValueCount(p);
      if (countValueCount == 0) {
        continue;
      }
      int upperBoundValueCount = upperBoundBlock.getValueCount(p);
      if (upperBoundValueCount == 0) {
        continue;
      }
      int countStart = countBlock.getFirstValueIndex(p);
      int countEnd = countStart + countValueCount;
      for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
        double countValue = countBlock.getDouble(countOffset);
        int upperBoundStart = upperBoundBlock.getFirstValueIndex(p);
        int upperBoundEnd = upperBoundStart + upperBoundValueCount;
        for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
          BytesRef upperBoundValue = upperBoundBlock.getBytesRef(upperBoundOffset, upperBoundScratch);
          PromqlHistogramQuantileAggregator.combine(state, countValue, upperBoundValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block bucketsUncast = page.getBlock(channels.get(0));
    if (bucketsUncast.areAllValuesNull()) {
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
    DoubleBlock buckets = (DoubleBlock) bucketsUncast;
    assert buckets.getPositionCount() == 1;
    PromqlHistogramQuantileAggregator.combineIntermediate(state, buckets);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = PromqlHistogramQuantileAggregator.evaluateFinal(state, driverContext);
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
