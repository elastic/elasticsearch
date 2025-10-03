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
import org.apache.lucene.util.RamUsageEstimator;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Sub}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class SubDateNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SubDateNanosEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator dateNanos;

  private final TemporalAmount temporalAmount;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SubDateNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator dateNanos,
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

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += dateNanos.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock dateNanosBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (dateNanosBlock.getValueCount(p)) {
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
        long dateNanos = dateNanosBlock.getLong(dateNanosBlock.getFirstValueIndex(p));
        try {
          result.appendLong(Sub.processDateNanos(dateNanos, this.temporalAmount));
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
        long dateNanos = dateNanosVector.getLong(p);
        try {
          result.appendLong(Sub.processDateNanos(dateNanos, this.temporalAmount));
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
    return "SubDateNanosEvaluator[" + "dateNanos=" + dateNanos + ", temporalAmount=" + temporalAmount + "]";
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
    public SubDateNanosEvaluator get(DriverContext context) {
      return new SubDateNanosEvaluator(source, dateNanos.get(context), temporalAmount, context);
    }

    @Override
    public String toString() {
      return "SubDateNanosEvaluator[" + "dateNanos=" + dateNanos + ", temporalAmount=" + temporalAmount + "]";
    }
  }
}
