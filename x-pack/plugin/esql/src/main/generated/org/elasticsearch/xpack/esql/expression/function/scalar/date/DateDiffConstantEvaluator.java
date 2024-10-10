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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateDiff}.
 * This class is generated. Do not edit it.
 */
public final class DateDiffConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final DateDiff.Part datePartFieldUnit;

  private final EvalOperator.ExpressionEvaluator startTimestamp;

  private final EvalOperator.ExpressionEvaluator endTimestamp;

  private final DriverContext driverContext;

  public DateDiffConstantEvaluator(Source source, DateDiff.Part datePartFieldUnit,
      EvalOperator.ExpressionEvaluator startTimestamp,
      EvalOperator.ExpressionEvaluator endTimestamp, DriverContext driverContext) {
    this.datePartFieldUnit = datePartFieldUnit;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
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
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
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
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendInt(DateDiff.process(this.datePartFieldUnit, startTimestampBlock.getLong(startTimestampBlock.getFirstValueIndex(p)), endTimestampBlock.getLong(endTimestampBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings.registerException(e);
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
          result.appendInt(DateDiff.process(this.datePartFieldUnit, startTimestampVector.getLong(p), endTimestampVector.getLong(p)));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateDiffConstantEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(startTimestamp, endTimestamp);
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
    public DateDiffConstantEvaluator get(DriverContext context) {
      return new DateDiffConstantEvaluator(source, datePartFieldUnit, startTimestamp.get(context), endTimestamp.get(context), context);
    }

    @Override
    public String toString() {
      return "DateDiffConstantEvaluator[" + "datePartFieldUnit=" + datePartFieldUnit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
    }
  }
}
