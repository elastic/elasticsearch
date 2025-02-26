// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.DateTimeException;
import java.time.temporal.TemporalAmount;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Add}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class AddDatetimesEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator datetime;

  private final TemporalAmount temporalAmount;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AddDatetimesEvaluator(Source source, EvalOperator.ExpressionEvaluator datetime,
      TemporalAmount temporalAmount, DriverContext driverContext) {
    this.source = source;
    this.datetime = datetime;
    this.temporalAmount = temporalAmount;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock datetimeBlock = (LongBlock) datetime.eval(page)) {
      LongVector datetimeVector = datetimeBlock.asVector();
      if (datetimeVector == null) {
        return eval(page.getPositionCount(), datetimeBlock);
      }
      return eval(page.getPositionCount(), datetimeVector);
    }
  }

  public LongBlock eval(int positionCount, LongBlock datetimeBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (datetimeBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (datetimeBlock.getValueCount(p) != 1) {
          if (datetimeBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendLong(Add.processDatetimes(datetimeBlock.getLong(datetimeBlock.getFirstValueIndex(p)), this.temporalAmount));
        } catch (ArithmeticException | DateTimeException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, LongVector datetimeVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendLong(Add.processDatetimes(datetimeVector.getLong(p), this.temporalAmount));
        } catch (ArithmeticException | DateTimeException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AddDatetimesEvaluator[" + "datetime=" + datetime + ", temporalAmount=" + temporalAmount + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(datetime);
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

    private final EvalOperator.ExpressionEvaluator.Factory datetime;

    private final TemporalAmount temporalAmount;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory datetime,
        TemporalAmount temporalAmount) {
      this.source = source;
      this.datetime = datetime;
      this.temporalAmount = temporalAmount;
    }

    @Override
    public AddDatetimesEvaluator get(DriverContext context) {
      return new AddDatetimesEvaluator(source, datetime.get(context), temporalAmount, context);
    }

    @Override
    public String toString() {
      return "AddDatetimesEvaluator[" + "datetime=" + datetime + ", temporalAmount=" + temporalAmount + "]";
    }
  }
}
