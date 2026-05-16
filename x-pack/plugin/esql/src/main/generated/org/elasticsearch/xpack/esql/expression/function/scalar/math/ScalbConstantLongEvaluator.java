// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
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
 * {@link ExpressionEvaluator} implementation for {@link Scalb}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class ScalbConstantLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ScalbConstantLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator d;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ScalbConstantLongEvaluator(Source source, ExpressionEvaluator d,
      DriverContext driverContext) {
    this.source = source;
    this.d = d;
    this.driverContext = driverContext;
  }

  protected abstract long scaleFactor();

  @Override
  public Block eval(Page page) {
    try (DoubleBlock dBlock = (DoubleBlock) d.eval(page)) {
      DoubleVector dVector = dBlock.asVector();
      if (dVector == null) {
        return eval(page.getPositionCount(), dBlock);
      }
      return eval(page.getPositionCount(), dVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += d.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock dBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (dBlock.getValueCount(p)) {
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
        double d = dBlock.getDouble(dBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Scalb.processConstantLong(d, scaleFactor()));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector dVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double d = dVector.getDouble(p);
        try {
          result.appendDouble(Scalb.processConstantLong(d, scaleFactor()));
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
    return "ScalbConstantLongEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor() + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(d);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory d;

    private final long scaleFactor;

    public Factory(Source source, ExpressionEvaluator.Factory d, long scaleFactor) {
      this.source = source;
      this.d = d;
      this.scaleFactor = scaleFactor;
    }

    @Override
    public ScalbConstantLongEvaluator get(DriverContext context) {
      Class<? extends ScalbConstantLongEvaluator> spunClass = JitConstantSpinner.longConstantSubclass(ScalbConstantLongEvaluator.class, "scaleFactor", this.scaleFactor).orElseThrow(() -> new IllegalStateException("JitConstantSpinner cache exhausted for ScalbConstantLongEvaluator value=" + this.scaleFactor));
      try {
        return (ScalbConstantLongEvaluator) spunClass.getDeclaredConstructors()[0].newInstance(source, d.get(context), context);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to construct JIT-spun evaluator for ScalbConstantLongEvaluator", e);
      }
    }

    @Override
    public String toString() {
      return "ScalbConstantLongEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor + "]";
    }
  }
}
