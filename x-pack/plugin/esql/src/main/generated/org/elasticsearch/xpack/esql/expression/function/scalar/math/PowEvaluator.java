// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class PowEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PowEvaluator.class);

  /**
   * The per-row function of this evaluator, callable through {@link #process}.
   */
  public static final EvaluatorKernel KERNEL = new EvaluatorKernel(PowEvaluator.class, double.class, List.of(EvaluatorKernel.value(double.class), EvaluatorKernel.value(double.class), EvaluatorKernel.fixed(boolean.class)), List.of(ArithmeticException.class));

  private final Source source;

  private final ExpressionEvaluator base;

  private final ExpressionEvaluator exponent;

  private final boolean allowNonFinite;

  private final DriverContext driverContext;

  private Warnings warnings;

  public PowEvaluator(Source source, ExpressionEvaluator base, ExpressionEvaluator exponent,
      boolean allowNonFinite, DriverContext driverContext) {
    this.source = source;
    this.base = base;
    this.exponent = exponent;
    this.allowNonFinite = allowNonFinite;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock baseBlock = (DoubleBlock) base.eval(page)) {
      try (DoubleBlock exponentBlock = (DoubleBlock) exponent.eval(page)) {
        DoubleVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        DoubleVector exponentVector = exponentBlock.asVector();
        if (exponentVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        return eval(page.getPositionCount(), baseVector, exponentVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += base.baseRamBytesUsed();
    baseRamBytesUsed += exponent.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  /**
   * The per-row function of this evaluator, described by {@link #KERNEL}.
   */
  public static double process(double base, double exponent, boolean allowNonFinite) {
    return Pow.process(base, exponent, allowNonFinite);
  }

  public DoubleBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock exponentBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (baseBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (baseBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (exponentBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (exponentBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double base = baseBlock.getDouble(baseBlock.getFirstValueIndex(p));
        double exponent = exponentBlock.getDouble(exponentBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Pow.process(base, exponent, this.allowNonFinite));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector baseVector, DoubleVector exponentVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double base = baseVector.getDouble(p);
        double exponent = exponentVector.getDouble(p);
        try {
          result.appendDouble(Pow.process(base, exponent, this.allowNonFinite));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(base, exponent);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements FusableEvaluatorFactory {
    private final Source source;

    private final ExpressionEvaluator.Factory base;

    private final ExpressionEvaluator.Factory exponent;

    private final boolean allowNonFinite;

    public Factory(Source source, ExpressionEvaluator.Factory base,
        ExpressionEvaluator.Factory exponent, boolean allowNonFinite) {
      this.source = source;
      this.base = base;
      this.exponent = exponent;
      this.allowNonFinite = allowNonFinite;
    }

    @Override
    public PowEvaluator get(DriverContext context) {
      return new PowEvaluator(source, base.get(context), exponent.get(context), allowNonFinite, context);
    }

    @Override
    public String toString() {
      return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
    }

    @Override
    public EvaluatorKernel kernel() {
      return KERNEL;
    }

    @Override
    public List<ExpressionEvaluator.Factory> inputs() {
      return List.of(base, exponent);
    }

    @Override
    public List<Object> fixedValues() {
      return Arrays.<Object>asList(allowNonFinite);
    }

    @Override
    public Source source() {
      return source;
    }
  }
}
