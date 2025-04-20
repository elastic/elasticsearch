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
public final class AddDateNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator dateNanos;

  private final TemporalAmount temporalAmount;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AddDateNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator dateNanos,
      TemporalAmount temporalAmount, DriverContext driverContext) {
    this.source = source;
    this.dateNanos = dateNanos;
    this.temporalAmount = temporalAmount;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock dateNanosBlock = (LongBlock) dateNanos.eval(page)) {
      LongVector dateNanosVector = dateNanosBlock.asVector();
      if (dateNanosVector == null) {
        return eval(page.getPositionCount(), dateNanosBlock);
      }
      return eval(page.getPositionCount(), dateNanosVector);
    }
  }

  public LongBlock eval(int positionCount, LongBlock dateNanosBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (dateNanosBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (dateNanosBlock.getValueCount(p) != 1) {
          if (dateNanosBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendLong(Add.processDateNanos(dateNanosBlock.getLong(dateNanosBlock.getFirstValueIndex(p)), this.temporalAmount));
        } catch (ArithmeticException | DateTimeException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public LongBlock eval(int positionCount, LongVector dateNanosVector) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendLong(Add.processDateNanos(dateNanosVector.getLong(p), this.temporalAmount));
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
    return "AddDateNanosEvaluator[" + "dateNanos=" + dateNanos + ", temporalAmount=" + temporalAmount + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(dateNanos);
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

    private final EvalOperator.ExpressionEvaluator.Factory dateNanos;

    private final TemporalAmount temporalAmount;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory dateNanos,
        TemporalAmount temporalAmount) {
      this.source = source;
      this.dateNanos = dateNanos;
      this.temporalAmount = temporalAmount;
    }

    @Override
    public AddDateNanosEvaluator get(DriverContext context) {
      return new AddDateNanosEvaluator(source, dateNanos.get(context), temporalAmount, context);
    }

    @Override
    public String toString() {
      return "AddDateNanosEvaluator[" + "dateNanos=" + dateNanos + ", temporalAmount=" + temporalAmount + "]";
    }
  }
}
