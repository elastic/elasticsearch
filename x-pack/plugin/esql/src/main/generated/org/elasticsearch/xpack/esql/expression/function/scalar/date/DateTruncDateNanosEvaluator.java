// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link DateTrunc}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class DateTruncDateNanosEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateTruncDateNanosEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator fieldVal;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateTruncDateNanosEvaluator(Source source, ExpressionEvaluator fieldVal,
      DriverContext driverContext) {
    this.source = source;
    this.fieldVal = fieldVal;
    this.driverContext = driverContext;
  }

  protected abstract Rounding.Prepared rounding();

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldValBlock = (LongBlock) fieldVal.eval(page)) {
      LongVector fieldValVector = fieldValBlock.asVector();
      if (fieldValVector == null) {
        return eval(page.getPositionCount(), fieldValBlock);
      }
      return eval(page.getPositionCount(), fieldValVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += fieldVal.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock fieldValBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (fieldValBlock.getValueCount(p)) {
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
        long fieldVal = fieldValBlock.getLong(fieldValBlock.getFirstValueIndex(p));
        result.appendLong(DateTrunc.processDateNanos(fieldVal, rounding()));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector fieldValVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long fieldVal = fieldValVector.getLong(p);
        result.appendLong(p, DateTrunc.processDateNanos(fieldVal, rounding()));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateTruncDateNanosEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding() + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory fieldVal;

    private final Rounding.Prepared rounding;

    public Factory(Source source, ExpressionEvaluator.Factory fieldVal,
        Rounding.Prepared rounding) {
      this.source = source;
      this.fieldVal = fieldVal;
      this.rounding = rounding;
    }

    @Override
    public DateTruncDateNanosEvaluator get(DriverContext context) {
      Class<? extends DateTruncDateNanosEvaluator> spunClass = JitConstantSpinner.referenceConstantSubclass(DateTruncDateNanosEvaluator.class, "rounding", Rounding.Prepared.class, this.rounding).orElseThrow(() -> new IllegalStateException("JitConstantSpinner cache exhausted for DateTruncDateNanosEvaluator value=" + this.rounding));
      try {
        return (DateTruncDateNanosEvaluator) spunClass.getDeclaredConstructors()[0].newInstance(source, fieldVal.get(context), context);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to construct JIT-spun evaluator for DateTruncDateNanosEvaluator", e);
      }
    }

    @Override
    public String toString() {
      return "DateTruncDateNanosEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding + "]";
    }
  }
}
