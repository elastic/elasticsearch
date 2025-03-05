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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class AbsDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator fieldVal;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AbsDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator fieldVal,
      DriverContext driverContext) {
    this.source = source;
    this.fieldVal = fieldVal;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock fieldValBlock = (DoubleBlock) fieldVal.eval(page)) {
      DoubleVector fieldValVector = fieldValBlock.asVector();
      if (fieldValVector == null) {
        return eval(page.getPositionCount(), fieldValBlock);
      }
      return eval(page.getPositionCount(), fieldValVector).asBlock();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock fieldValBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldValBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (fieldValBlock.getValueCount(p) != 1) {
          if (fieldValBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Abs.process(fieldValBlock.getDouble(fieldValBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector fieldValVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Abs.process(fieldValVector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AbsDoubleEvaluator[" + "fieldVal=" + fieldVal + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
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

    private final EvalOperator.ExpressionEvaluator.Factory fieldVal;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldVal) {
      this.source = source;
      this.fieldVal = fieldVal;
    }

    @Override
    public AbsDoubleEvaluator get(DriverContext context) {
      return new AbsDoubleEvaluator(source, fieldVal.get(context), context);
    }

    @Override
    public String toString() {
      return "AbsDoubleEvaluator[" + "fieldVal=" + fieldVal + "]";
    }
  }
}
