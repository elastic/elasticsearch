// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Scalb}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ScalbLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator d;

  private final EvalOperator.ExpressionEvaluator scaleFactor;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ScalbLongEvaluator(Source source, EvalOperator.ExpressionEvaluator d,
      EvalOperator.ExpressionEvaluator scaleFactor, DriverContext driverContext) {
    this.source = source;
    this.d = d;
    this.scaleFactor = scaleFactor;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock dBlock = (DoubleBlock) d.eval(page)) {
      try (LongBlock scaleFactorBlock = (LongBlock) scaleFactor.eval(page)) {
        DoubleVector dVector = dBlock.asVector();
        if (dVector == null) {
          return eval(page.getPositionCount(), dBlock, scaleFactorBlock);
        }
        LongVector scaleFactorVector = scaleFactorBlock.asVector();
        if (scaleFactorVector == null) {
          return eval(page.getPositionCount(), dBlock, scaleFactorBlock);
        }
        return eval(page.getPositionCount(), dVector, scaleFactorVector);
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock dBlock, LongBlock scaleFactorBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (dBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (dBlock.getValueCount(p) != 1) {
          if (dBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (scaleFactorBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (scaleFactorBlock.getValueCount(p) != 1) {
          if (scaleFactorBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendDouble(Scalb.process(dBlock.getDouble(dBlock.getFirstValueIndex(p)), scaleFactorBlock.getLong(scaleFactorBlock.getFirstValueIndex(p))));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector dVector, LongVector scaleFactorVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(Scalb.process(dVector.getDouble(p), scaleFactorVector.getLong(p)));
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
    return "ScalbLongEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(d, scaleFactor);
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

    private final EvalOperator.ExpressionEvaluator.Factory d;

    private final EvalOperator.ExpressionEvaluator.Factory scaleFactor;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory d,
        EvalOperator.ExpressionEvaluator.Factory scaleFactor) {
      this.source = source;
      this.d = d;
      this.scaleFactor = scaleFactor;
    }

    @Override
    public ScalbLongEvaluator get(DriverContext context) {
      return new ScalbLongEvaluator(source, d.get(context), scaleFactor.get(context), context);
    }

    @Override
    public String toString() {
      return "ScalbLongEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor + "]";
    }
  }
}
