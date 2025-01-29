// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Atan2}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class Atan2Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator y;

  private final EvalOperator.ExpressionEvaluator x;

  private final DriverContext driverContext;

  private Warnings warnings;

  public Atan2Evaluator(Source source, EvalOperator.ExpressionEvaluator y,
      EvalOperator.ExpressionEvaluator x, DriverContext driverContext) {
    this.source = source;
    this.y = y;
    this.x = x;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock yBlock = (DoubleBlock) y.eval(page)) {
      try (DoubleBlock xBlock = (DoubleBlock) x.eval(page)) {
        DoubleVector yVector = yBlock.asVector();
        if (yVector == null) {
          return eval(page.getPositionCount(), yBlock, xBlock);
        }
        DoubleVector xVector = xBlock.asVector();
        if (xVector == null) {
          return eval(page.getPositionCount(), yBlock, xBlock);
        }
        return eval(page.getPositionCount(), yVector, xVector).asBlock();
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock yBlock, DoubleBlock xBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (yBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (yBlock.getValueCount(p) != 1) {
          if (yBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (xBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (xBlock.getValueCount(p) != 1) {
          if (xBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Atan2.process(yBlock.getDouble(yBlock.getFirstValueIndex(p)), xBlock.getDouble(xBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector yVector, DoubleVector xVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Atan2.process(yVector.getDouble(p), xVector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "Atan2Evaluator[" + "y=" + y + ", x=" + x + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(y, x);
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

    private final EvalOperator.ExpressionEvaluator.Factory y;

    private final EvalOperator.ExpressionEvaluator.Factory x;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory y,
        EvalOperator.ExpressionEvaluator.Factory x) {
      this.source = source;
      this.y = y;
      this.x = x;
    }

    @Override
    public Atan2Evaluator get(DriverContext context) {
      return new Atan2Evaluator(source, y.get(context), x.get(context), context);
    }

    @Override
    public String toString() {
      return "Atan2Evaluator[" + "y=" + y + ", x=" + x + "]";
    }
  }
}
