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
public final class ScalbConstantIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator d;

  private final int scaleFactor;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ScalbConstantIntEvaluator(Source source, EvalOperator.ExpressionEvaluator d,
      int scaleFactor, DriverContext driverContext) {
    this.source = source;
    this.d = d;
    this.scaleFactor = scaleFactor;
    this.driverContext = driverContext;
  }

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

  public DoubleBlock eval(int positionCount, DoubleBlock dBlock) {
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
        try {
          result.appendDouble(Scalb.processConstantInt(dBlock.getDouble(dBlock.getFirstValueIndex(p)), this.scaleFactor));
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
        try {
          result.appendDouble(Scalb.processConstantInt(dVector.getDouble(p), this.scaleFactor));
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
    return "ScalbConstantIntEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(d);
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

    private final int scaleFactor;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory d, int scaleFactor) {
      this.source = source;
      this.d = d;
      this.scaleFactor = scaleFactor;
    }

    @Override
    public ScalbConstantIntEvaluator get(DriverContext context) {
      return new ScalbConstantIntEvaluator(source, d.get(context), scaleFactor, context);
    }

    @Override
    public String toString() {
      return "ScalbConstantIntEvaluator[" + "d=" + d + ", scaleFactor=" + scaleFactor + "]";
    }
  }
}
