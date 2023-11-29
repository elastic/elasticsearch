// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Mul}.
 * This class is generated. Do not edit it.
 */
public final class MulDoublesEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  private final DriverContext driverContext;

  public MulDoublesEvaluator(EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs, DriverContext driverContext) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock lhsBlock = (DoubleBlock) lhs.eval(page)) {
      try (DoubleBlock rhsBlock = (DoubleBlock) rhs.eval(page)) {
        DoubleVector lhsVector = lhsBlock.asVector();
        if (lhsVector == null) {
          return eval(page.getPositionCount(), lhsBlock, rhsBlock);
        }
        DoubleVector rhsVector = rhsBlock.asVector();
        if (rhsVector == null) {
          return eval(page.getPositionCount(), lhsBlock, rhsBlock);
        }
        return eval(page.getPositionCount(), lhsVector, rhsVector).asBlock();
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock lhsBlock, DoubleBlock rhsBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (lhsBlock.isNull(p) || lhsBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        if (rhsBlock.isNull(p) || rhsBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendDouble(Mul.processDoubles(lhsBlock.getDouble(lhsBlock.getFirstValueIndex(p)), rhsBlock.getDouble(rhsBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector lhsVector, DoubleVector rhsVector) {
    try(DoubleVector.Builder result = driverContext.blockFactory().newDoubleVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(Mul.processDoubles(lhsVector.getDouble(p), rhsVector.getDouble(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MulDoublesEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs, rhs);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory lhs;

    private final EvalOperator.ExpressionEvaluator.Factory rhs;

    public Factory(EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory rhs) {
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public MulDoublesEvaluator get(DriverContext context) {
      return new MulDoublesEvaluator(lhs.get(context), rhs.get(context), context);
    }

    @Override
    public String toString() {
      return "MulDoublesEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
