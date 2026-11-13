// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import java.util.BitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntSingleArrayPool;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link DateDiff}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateDiffConstantMillisNanosEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffConstantMillisNanosEvaluator.class);

  private final Source source;

  private final DateDiff.Part datePartFieldUnit;

  private final ExpressionEvaluator startTimestampMillis;

  private final ExpressionEvaluator endTimestampNanos;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  private final IntSingleArrayPool pool;

  public DateDiffConstantMillisNanosEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      ExpressionEvaluator startTimestampMillis, ExpressionEvaluator endTimestampNanos,
      ZoneId zoneId, DriverContext driverContext) {
    this.source = source;
    this.datePartFieldUnit = datePartFieldUnit;
    this.startTimestampMillis = startTimestampMillis;
    this.endTimestampNanos = endTimestampNanos;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
    this.pool = new IntSingleArrayPool(driverContext.breaker());
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock startTimestampMillisBlock = (LongBlock) startTimestampMillis.eval(page)) {
      try (LongBlock endTimestampNanosBlock = (LongBlock) endTimestampNanos.eval(page)) {
        LongVector startTimestampMillisVector = startTimestampMillisBlock.asVector();
        if (startTimestampMillisVector == null) {
          return eval(page.getPositionCount(), startTimestampMillisBlock, endTimestampNanosBlock);
        }
        LongVector endTimestampNanosVector = endTimestampNanosBlock.asVector();
        if (endTimestampNanosVector == null) {
          return eval(page.getPositionCount(), startTimestampMillisBlock, endTimestampNanosBlock);
        }
        return eval(page.getPositionCount(), startTimestampMillisVector, endTimestampNanosVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += startTimestampMillis.baseRamBytesUsed();
    baseRamBytesUsed += endTimestampNanos.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, LongBlock startTimestampMillisBlock,
      LongBlock endTimestampNanosBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (startTimestampMillisBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (startTimestampMillisBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (endTimestampNanosBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (endTimestampNanosBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long startTimestampMillis = startTimestampMillisBlock.getLong(startTimestampMillisBlock.getFirstValueIndex(p));
        long endTimestampNanos = endTimestampNanosBlock.getLong(endTimestampNanosBlock.getFirstValueIndex(p));
        try {
          result.appendInt(DateDiff.processMillisNanos(this.datePartFieldUnit, startTimestampMillis, endTimestampNanos, this.zoneId));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, LongVector startTimestampMillisVector,
      LongVector endTimestampNanosVector) {
    final IntSingleArrayPool pool = this.pool;
    final int[] results = pool.getOrAllocate(positionCount);
    BitSet nulls = null;
    position: for (int p = 0; p < positionCount; p++) {
      long startTimestampMillis = startTimestampMillisVector.getLong(p);
      long endTimestampNanos = endTimestampNanosVector.getLong(p);
      try {
        results[p] = DateDiff.processMillisNanos(this.datePartFieldUnit, startTimestampMillis, endTimestampNanos, this.zoneId);
      } catch (IllegalArgumentException | InvalidArgumentException e) {
        warnings().registerException(e);
        if (nulls == null) {
          nulls = new BitSet(positionCount);
        }
        nulls.set(p);
      }
    }
    if (nulls != null) {
      IntBlock block = driverContext.blockFactory().newIntArrayBlock(results, positionCount, null, nulls, Block.MvOrdering.UNORDERED);
      block.attachReleasable(() -> pool.put(results));
      return block;
    }
    IntVector result = driverContext.blockFactory().newIntArrayVector(results, positionCount);
    result.attachReleasable(() -> pool.put(results));
    return result.asBlock();
  }

  @Override
  public String toString() {
    return "DateDiffConstantMillisNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(startTimestampMillis, endTimestampNanos);
    Releasables.closeExpectNoException(pool);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final DateDiff.Part datePartFieldUnit;

    private final ExpressionEvaluator.Factory startTimestampMillis;

    private final ExpressionEvaluator.Factory endTimestampNanos;

    private final ZoneId zoneId;

    public Factory(Source source, DateDiff.Part datePartFieldUnit,
        ExpressionEvaluator.Factory startTimestampMillis,
        ExpressionEvaluator.Factory endTimestampNanos, ZoneId zoneId) {
      this.source = source;
      this.datePartFieldUnit = datePartFieldUnit;
      this.startTimestampMillis = startTimestampMillis;
      this.endTimestampNanos = endTimestampNanos;
      this.zoneId = zoneId;
    }

    @Override
    public DateDiffConstantMillisNanosEvaluator get(DriverContext context) {
      return new DateDiffConstantMillisNanosEvaluator(source, datePartFieldUnit, startTimestampMillis.get(context), endTimestampNanos.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateDiffConstantMillisNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + ", zoneId=" + zoneId + "]";
    }
  }
}
