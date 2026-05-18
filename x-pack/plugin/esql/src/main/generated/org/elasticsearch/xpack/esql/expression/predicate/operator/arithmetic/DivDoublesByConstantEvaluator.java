// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.ArithmeticException;
import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Div}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class DivDoublesByConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DivDoublesByConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator lhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DivDoublesByConstantEvaluator(Source source, ExpressionEvaluator lhs,
      DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.driverContext = driverContext;
  }

  protected abstract double rhs();

  @Override
  public Block eval(Page page) {
    try (DoubleBlock lhsBlock = (DoubleBlock) lhs.eval(page)) {
      DoubleVector lhsVector = lhsBlock.asVector();
      if (lhsVector == null) {
        return eval(page.getPositionCount(), lhsBlock);
      }
      return eval(page.getPositionCount(), lhsVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += lhs.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock lhsBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (lhsBlock.getValueCount(p)) {
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
        double lhs = lhsBlock.getDouble(lhsBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Div.processDoublesByConstant(lhs, rhs()));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector lhsVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double lhs = lhsVector.getDouble(p);
        try {
          result.appendDouble(Div.processDoublesByConstant(lhs, rhs()));
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
    return "DivDoublesByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs() + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory lhs;

    private final double rhs;

    public Factory(Source source, ExpressionEvaluator.Factory lhs, double rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public DivDoublesByConstantEvaluator get(DriverContext context) {
      Optional<Class<? extends DivDoublesByConstantEvaluator>> spunClassOpt = JitConstantSpinner.doubleConstantSubclass(DivDoublesByConstantEvaluator.class, "rhs", this.rhs);
      if (spunClassOpt.isPresent()) {
        Class<? extends DivDoublesByConstantEvaluator> spunClass = spunClassOpt.get();
        try {
          return (DivDoublesByConstantEvaluator) spunClass.getConstructors()[0].newInstance(source, lhs.get(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct JIT-spun evaluator for DivDoublesByConstantEvaluator", e);
        }
      }
      return new Fallback(source, lhs.get(context), this.rhs, context);
    }

    @Override
    public String toString() {
      return "DivDoublesByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }

  /**
   * Concrete fallback used when {@link JitConstantSpinner} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the spun subclass.
   */
  public static final class Fallback extends DivDoublesByConstantEvaluator {
    private final double rhs;

    public Fallback(Source source, ExpressionEvaluator lhs, double rhs,
        DriverContext driverContext) {
      super(source, lhs, driverContext);
      this.rhs = rhs;
    }

    @Override
    protected final double rhs() {
      return rhs;
    }
  }
}
