// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateDiff}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateDiffConstantNanosMillisEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffConstantNanosMillisEvaluator.class);

  private final Source source;

  private final DateDiff.Part datePartFieldUnit;

  private final EvalOperator.ExpressionEvaluator startTimestampNanos;

  private final EvalOperator.ExpressionEvaluator endTimestampMillis;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffConstantNanosMillisEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      EvalOperator.ExpressionEvaluator startTimestampNanos,
      EvalOperator.ExpressionEvaluator endTimestampMillis, ZoneId zoneId,
      DriverContext driverContext) {
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

  public IntBlock eval(int positionCount, LongBlock startTimestampNanosBlock,
      LongBlock endTimestampMillisBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (startTimestampNanosBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (endTimestampMillisBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
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
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final DateDiff.Part datePartFieldUnit;

    private final EvalOperator.ExpressionEvaluator.Factory startTimestampNanos;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestampMillis;

    private final ZoneId zoneId;

    public Factory(Source source, DateDiff.Part datePartFieldUnit,
        EvalOperator.ExpressionEvaluator.Factory startTimestampNanos,
        EvalOperator.ExpressionEvaluator.Factory endTimestampMillis, ZoneId zoneId) {
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
  }
}
