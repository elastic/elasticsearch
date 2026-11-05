// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link DateTrunc}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateTruncDateNanosEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateTruncDateNanosEvaluator.class);

  /**
   * The per-row function of this evaluator, callable through {@link #process}.
   */
  public static final EvaluatorKernel KERNEL = new EvaluatorKernel(DateTruncDateNanosEvaluator.class, long.class, List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.fixed(Rounding.Prepared.class)), List.of());

  private final Source source;

  private final ExpressionEvaluator fieldVal;

  private final Rounding.Prepared rounding;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateTruncDateNanosEvaluator(Source source, ExpressionEvaluator fieldVal,
      Rounding.Prepared rounding, DriverContext driverContext) {
    this.source = source;
    this.fieldVal = fieldVal;
    this.rounding = rounding;
    this.driverContext = driverContext;
  }

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

  /**
   * The per-row function of this evaluator, described by {@link #KERNEL}.
   */
  public static long process(long fieldVal, Rounding.Prepared rounding) {
    return DateTrunc.processDateNanos(fieldVal, rounding);
  }

  public LongBlock eval(int positionCount, LongBlock fieldValBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldValBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (fieldValBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long fieldVal = fieldValBlock.getLong(fieldValBlock.getFirstValueIndex(p));
        result.appendLong(DateTrunc.processDateNanos(fieldVal, this.rounding));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector fieldValVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long fieldVal = fieldValVector.getLong(p);
        result.appendLong(p, DateTrunc.processDateNanos(fieldVal, this.rounding));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateTruncDateNanosEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements FusableEvaluatorFactory {
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
      return new DateTruncDateNanosEvaluator(source, fieldVal.get(context), rounding, context);
    }

    @Override
    public String toString() {
      return "DateTruncDateNanosEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding + "]";
    }

    @Override
    public EvaluatorKernel kernel() {
      return KERNEL;
    }

    @Override
    public List<ExpressionEvaluator.Factory> inputs() {
      return List.of(fieldVal);
    }

    @Override
    public List<Object> fixedValues() {
      return Arrays.<Object>asList(rounding);
    }

    @Override
    public Source source() {
      return source;
    }
  }
}
