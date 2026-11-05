// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link DateDiff}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateDiffConstantNanosMillisEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffConstantNanosMillisEvaluator.class);

  /**
   * The per-row function of this evaluator, callable through {@link #process}.
   */
  public static final EvaluatorKernel KERNEL = new EvaluatorKernel(DateDiffConstantNanosMillisEvaluator.class, int.class, List.of(EvaluatorKernel.fixed(DateDiff.Part.class), EvaluatorKernel.value(long.class), EvaluatorKernel.value(long.class), EvaluatorKernel.fixed(ZoneId.class)), List.of(IllegalArgumentException.class, InvalidArgumentException.class));

  private final Source source;

  private final DateDiff.Part datePartFieldUnit;

  private final ExpressionEvaluator startTimestampNanos;

  private final ExpressionEvaluator endTimestampMillis;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffConstantNanosMillisEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      ExpressionEvaluator startTimestampNanos, ExpressionEvaluator endTimestampMillis,
      ZoneId zoneId, DriverContext driverContext) {
    this.source = source;
    this.datePartFieldUnit = datePartFieldUnit;
    this.startTimestampNanos = startTimestampNanos;
    this.endTimestampMillis = endTimestampMillis;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock startTimestampNanosBlock = (LongBlock) startTimestampNanos.eval(page)) {
      try (LongBlock endTimestampMillisBlock = (LongBlock) endTimestampMillis.eval(page)) {
        LongVector startTimestampNanosVector = startTimestampNanosBlock.asVector();
        if (startTimestampNanosVector == null) {
          return eval(page.getPositionCount(), startTimestampNanosBlock, endTimestampMillisBlock);
        }
        LongVector endTimestampMillisVector = endTimestampMillisBlock.asVector();
        if (endTimestampMillisVector == null) {
          return eval(page.getPositionCount(), startTimestampNanosBlock, endTimestampMillisBlock);
        }
        return eval(page.getPositionCount(), startTimestampNanosVector, endTimestampMillisVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += startTimestampNanos.baseRamBytesUsed();
    baseRamBytesUsed += endTimestampMillis.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  /**
   * The per-row function of this evaluator, described by {@link #KERNEL}.
   */
  public static int process(DateDiff.Part datePartFieldUnit, long startTimestampNanos,
      long endTimestampMillis, ZoneId zoneId) {
    return DateDiff.processNanosMillis(datePartFieldUnit, startTimestampNanos, endTimestampMillis, zoneId);
  }

  public IntBlock eval(int positionCount, LongBlock startTimestampNanosBlock,
      LongBlock endTimestampMillisBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (startTimestampNanosBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (startTimestampNanosBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (endTimestampMillisBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (endTimestampMillisBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long startTimestampNanos = startTimestampNanosBlock.getLong(startTimestampNanosBlock.getFirstValueIndex(p));
        long endTimestampMillis = endTimestampMillisBlock.getLong(endTimestampMillisBlock.getFirstValueIndex(p));
        try {
          result.appendInt(DateDiff.processNanosMillis(this.datePartFieldUnit, startTimestampNanos, endTimestampMillis, this.zoneId));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, LongVector startTimestampNanosVector,
      LongVector endTimestampMillisVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long startTimestampNanos = startTimestampNanosVector.getLong(p);
        long endTimestampMillis = endTimestampMillisVector.getLong(p);
        try {
          result.appendInt(DateDiff.processNanosMillis(this.datePartFieldUnit, startTimestampNanos, endTimestampMillis, this.zoneId));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateDiffConstantNanosMillisEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampNanos=" + startTimestampNanos + ", endTimestampMillis=" + endTimestampMillis + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(startTimestampNanos, endTimestampMillis);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements FusableEvaluatorFactory {
    private final Source source;

    private final DateDiff.Part datePartFieldUnit;

    private final ExpressionEvaluator.Factory startTimestampNanos;

    private final ExpressionEvaluator.Factory endTimestampMillis;

    private final ZoneId zoneId;

    public Factory(Source source, DateDiff.Part datePartFieldUnit,
        ExpressionEvaluator.Factory startTimestampNanos,
        ExpressionEvaluator.Factory endTimestampMillis, ZoneId zoneId) {
      this.source = source;
      this.datePartFieldUnit = datePartFieldUnit;
      this.startTimestampNanos = startTimestampNanos;
      this.endTimestampMillis = endTimestampMillis;
      this.zoneId = zoneId;
    }

    @Override
    public DateDiffConstantNanosMillisEvaluator get(DriverContext context) {
      return new DateDiffConstantNanosMillisEvaluator(source, datePartFieldUnit, startTimestampNanos.get(context), endTimestampMillis.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateDiffConstantNanosMillisEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampNanos=" + startTimestampNanos + ", endTimestampMillis=" + endTimestampMillis + ", zoneId=" + zoneId + "]";
    }

    @Override
    public EvaluatorKernel kernel() {
      return KERNEL;
    }

    @Override
    public List<ExpressionEvaluator.Factory> inputs() {
      return List.of(startTimestampNanos, endTimestampMillis);
    }

    @Override
    public List<Object> fixedValues() {
      return Arrays.<Object>asList(datePartFieldUnit, zoneId);
    }

    @Override
    public Source source() {
      return source;
    }
  }
}
