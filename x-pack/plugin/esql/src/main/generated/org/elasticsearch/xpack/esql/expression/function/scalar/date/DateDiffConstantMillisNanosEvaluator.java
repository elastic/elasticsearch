// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
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
public final class DateDiffConstantMillisNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final DateDiff.Part datePartFieldUnit;

  private final EvalOperator.ExpressionEvaluator startTimestampMillis;

  private final EvalOperator.ExpressionEvaluator endTimestampNanos;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffConstantMillisNanosEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      EvalOperator.ExpressionEvaluator startTimestampMillis,
      EvalOperator.ExpressionEvaluator endTimestampNanos, DriverContext driverContext) {
    this.source = source;
    this.datePartFieldUnit = datePartFieldUnit;
    this.startTimestampMillis = startTimestampMillis;
    this.endTimestampNanos = endTimestampNanos;
    this.driverContext = driverContext;
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

  public IntBlock eval(int positionCount, LongBlock startTimestampMillisBlock,
      LongBlock endTimestampNanosBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (startTimestampMillisBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (startTimestampMillisBlock.getValueCount(p) != 1) {
          if (startTimestampMillisBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (endTimestampNanosBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (endTimestampNanosBlock.getValueCount(p) != 1) {
          if (endTimestampNanosBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendInt(DateDiff.processMillisNanos(this.datePartFieldUnit, startTimestampMillisBlock.getLong(startTimestampMillisBlock.getFirstValueIndex(p)), endTimestampNanosBlock.getLong(endTimestampNanosBlock.getFirstValueIndex(p))));
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
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendInt(DateDiff.processMillisNanos(this.datePartFieldUnit, startTimestampMillisVector.getLong(p), endTimestampNanosVector.getLong(p)));
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
    return "DateDiffConstantMillisNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(startTimestampMillis, endTimestampNanos);
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

    private final EvalOperator.ExpressionEvaluator.Factory startTimestampMillis;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestampNanos;

    public Factory(Source source, DateDiff.Part datePartFieldUnit,
        EvalOperator.ExpressionEvaluator.Factory startTimestampMillis,
        EvalOperator.ExpressionEvaluator.Factory endTimestampNanos) {
      this.source = source;
      this.datePartFieldUnit = datePartFieldUnit;
      this.startTimestampMillis = startTimestampMillis;
      this.endTimestampNanos = endTimestampNanos;
    }

    @Override
    public DateDiffConstantMillisNanosEvaluator get(DriverContext context) {
      return new DateDiffConstantMillisNanosEvaluator(source, datePartFieldUnit, startTimestampMillis.get(context), endTimestampNanos.get(context), context);
    }

    @Override
    public String toString() {
      return "DateDiffConstantMillisNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + "]";
    }
  }
}
