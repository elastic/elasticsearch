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
public final class DateDiffConstantNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final DateDiff.Part datePartFieldUnit;

  private final EvalOperator.ExpressionEvaluator startTimestamp;

  private final EvalOperator.ExpressionEvaluator endTimestamp;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffConstantNanosEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      EvalOperator.ExpressionEvaluator startTimestamp,
      EvalOperator.ExpressionEvaluator endTimestamp, DriverContext driverContext) {
    this.source = source;
    this.datePartFieldUnit = datePartFieldUnit;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock startTimestampBlock = (LongBlock) startTimestamp.eval(page)) {
      try (LongBlock endTimestampBlock = (LongBlock) endTimestamp.eval(page)) {
        LongVector startTimestampVector = startTimestampBlock.asVector();
        if (startTimestampVector == null) {
          return eval(page.getPositionCount(), startTimestampBlock, endTimestampBlock);
        }
        LongVector endTimestampVector = endTimestampBlock.asVector();
        if (endTimestampVector == null) {
          return eval(page.getPositionCount(), startTimestampBlock, endTimestampBlock);
        }
        return eval(page.getPositionCount(), startTimestampVector, endTimestampVector);
      }
    }
  }

  public IntBlock eval(int positionCount, LongBlock startTimestampBlock,
      LongBlock endTimestampBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (startTimestampBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (startTimestampBlock.getValueCount(p) != 1) {
          if (startTimestampBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (endTimestampBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (endTimestampBlock.getValueCount(p) != 1) {
          if (endTimestampBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendInt(DateDiff.processNanos(this.datePartFieldUnit, startTimestampBlock.getLong(startTimestampBlock.getFirstValueIndex(p)), endTimestampBlock.getLong(endTimestampBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, LongVector startTimestampVector,
      LongVector endTimestampVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendInt(DateDiff.processNanos(this.datePartFieldUnit, startTimestampVector.getLong(p), endTimestampVector.getLong(p)));
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
    return "DateDiffConstantNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(startTimestamp, endTimestamp);
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

    private final EvalOperator.ExpressionEvaluator.Factory startTimestamp;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestamp;

    public Factory(Source source, DateDiff.Part datePartFieldUnit,
        EvalOperator.ExpressionEvaluator.Factory startTimestamp,
        EvalOperator.ExpressionEvaluator.Factory endTimestamp) {
      this.source = source;
      this.datePartFieldUnit = datePartFieldUnit;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
    }

    @Override
    public DateDiffConstantNanosEvaluator get(DriverContext context) {
      return new DateDiffConstantNanosEvaluator(source, datePartFieldUnit, startTimestamp.get(context), endTimestamp.get(context), context);
    }

    @Override
    public String toString() {
      return "DateDiffConstantNanosEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
    }
  }
}
