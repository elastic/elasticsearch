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
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link SparklineLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SparklineLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("date", ElementType.LONG),
      new IntermediateStateDesc("trend", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final SparklineLongAggregator.SingleState state;

  private final List<Integer> channels;

  private final Rounding.Prepared dateBucketRounding;

  private final long minDate;

  private final long maxDate;

  private final AggregatorFunctionSupplier supplier;

  public SparklineLongAggregatorFunction(DriverContext driverContext, List<Integer> channels,
      SparklineLongAggregator.SingleState state, Rounding.Prepared dateBucketRounding, long minDate,
      long maxDate, AggregatorFunctionSupplier supplier) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.state = state;
    this.dateBucketRounding = dateBucketRounding;
    this.minDate = minDate;
    this.maxDate = maxDate;
    this.supplier = supplier;
  }

  public static SparklineLongAggregatorFunction create(DriverContext driverContext,
      List<Integer> channels, Rounding.Prepared dateBucketRounding, long minDate, long maxDate,
      AggregatorFunctionSupplier supplier) {
    return new SparklineLongAggregatorFunction(driverContext, channels, SparklineLongAggregator.initSingle(dateBucketRounding, minDate, maxDate, supplier), dateBucketRounding, minDate, maxDate, supplier);
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
    LongBlock trendValueBlock = page.getBlock(channels.get(0));
    LongBlock dateValueBlock = page.getBlock(channels.get(1));
    LongVector trendValueVector = trendValueBlock.asVector();
    if (trendValueVector == null) {
      addRawBlock(trendValueBlock, dateValueBlock, mask);
      return;
    }
    LongVector dateValueVector = dateValueBlock.asVector();
    if (dateValueVector == null) {
      addRawBlock(trendValueBlock, dateValueBlock, mask);
      return;
    }
    addRawVector(trendValueVector, dateValueVector, mask);
  }

  private void addRawInputNotMasked(Page page) {
    LongBlock trendValueBlock = page.getBlock(channels.get(0));
    LongBlock dateValueBlock = page.getBlock(channels.get(1));
    LongVector trendValueVector = trendValueBlock.asVector();
    if (trendValueVector == null) {
      addRawBlock(trendValueBlock, dateValueBlock);
      return;
    }
    LongVector dateValueVector = dateValueBlock.asVector();
    if (dateValueVector == null) {
      addRawBlock(trendValueBlock, dateValueBlock);
      return;
    }
    addRawVector(trendValueVector, dateValueVector);
  }

  private void addRawVector(LongVector trendValueVector, LongVector dateValueVector) {
    for (int valuesPosition = 0; valuesPosition < trendValueVector.getPositionCount(); valuesPosition++) {
      long trendValueValue = trendValueVector.getLong(valuesPosition);
      long dateValueValue = dateValueVector.getLong(valuesPosition);
      SparklineLongAggregator.combine(state, trendValueValue, dateValueValue);
    }
  }

  private void addRawVector(LongVector trendValueVector, LongVector dateValueVector,
      BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < trendValueVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long trendValueValue = trendValueVector.getLong(valuesPosition);
      long dateValueValue = dateValueVector.getLong(valuesPosition);
      SparklineLongAggregator.combine(state, trendValueValue, dateValueValue);
    }
  }

  private void addRawBlock(LongBlock trendValueBlock, LongBlock dateValueBlock) {
    for (int p = 0; p < trendValueBlock.getPositionCount(); p++) {
      int trendValueValueCount = trendValueBlock.getValueCount(p);
      if (trendValueValueCount == 0) {
        continue;
      }
      int dateValueValueCount = dateValueBlock.getValueCount(p);
      if (dateValueValueCount == 0) {
        continue;
      }
      int trendValueStart = trendValueBlock.getFirstValueIndex(p);
      int trendValueEnd = trendValueStart + trendValueValueCount;
      for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
        long trendValueValue = trendValueBlock.getLong(trendValueOffset);
        int dateValueStart = dateValueBlock.getFirstValueIndex(p);
        int dateValueEnd = dateValueStart + dateValueValueCount;
        for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
          long dateValueValue = dateValueBlock.getLong(dateValueOffset);
          SparklineLongAggregator.combine(state, trendValueValue, dateValueValue);
        }
      }
    }
  }

  private void addRawBlock(LongBlock trendValueBlock, LongBlock dateValueBlock,
      BooleanVector mask) {
    for (int p = 0; p < trendValueBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int trendValueValueCount = trendValueBlock.getValueCount(p);
      if (trendValueValueCount == 0) {
        continue;
      }
      int dateValueValueCount = dateValueBlock.getValueCount(p);
      if (dateValueValueCount == 0) {
        continue;
      }
      int trendValueStart = trendValueBlock.getFirstValueIndex(p);
      int trendValueEnd = trendValueStart + trendValueValueCount;
      for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
        long trendValueValue = trendValueBlock.getLong(trendValueOffset);
        int dateValueStart = dateValueBlock.getFirstValueIndex(p);
        int dateValueEnd = dateValueStart + dateValueValueCount;
        for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
          long dateValueValue = dateValueBlock.getLong(dateValueOffset);
          SparklineLongAggregator.combine(state, trendValueValue, dateValueValue);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert channels.size() == intermediateBlockCount();
    assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
    Block dateUncast = page.getBlock(channels.get(0));
    if (dateUncast.areAllValuesNull()) {
      return;
    }
    LongBlock date = (LongBlock) dateUncast;
    assert date.getPositionCount() == 1;
    Block trendUncast = page.getBlock(channels.get(1));
    if (trendUncast.areAllValuesNull()) {
      return;
    }
    LongBlock trend = (LongBlock) trendUncast;
    assert trend.getPositionCount() == 1;
    SparklineLongAggregator.combineIntermediate(state, date, trend);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SparklineLongAggregator.evaluateFinal(state, driverContext);
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
