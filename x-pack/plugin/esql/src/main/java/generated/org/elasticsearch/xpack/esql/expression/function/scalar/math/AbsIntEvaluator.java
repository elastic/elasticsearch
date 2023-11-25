// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Do not edit it.
 */
public final class AbsIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  private final DriverContext driverContext;

  public AbsIntEvaluator(EvalOperator.ExpressionEvaluator fieldVal, DriverContext driverContext) {
    this.fieldVal = fieldVal;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock fieldValBlock = (IntBlock) fieldVal.eval(page)) {
      IntVector fieldValVector = fieldValBlock.asVector();
      if (fieldValVector == null) {
        return eval(page.getPositionCount(), fieldValBlock);
      }
      return eval(page.getPositionCount(), fieldValVector).asBlock();
    }
  }

  public IntBlock eval(int positionCount, IntBlock fieldValBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldValBlock.isNull(p) || fieldValBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendInt(Abs.process(fieldValBlock.getInt(fieldValBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector fieldValVector) {
    try(IntVector.Builder result = driverContext.blockFactory().newIntVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendInt(Abs.process(fieldValVector.getInt(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AbsIntEvaluator[" + "fieldVal=" + fieldVal + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory fieldVal;

    public Factory(EvalOperator.ExpressionEvaluator.Factory fieldVal) {
      this.fieldVal = fieldVal;
    }

    @Override
    public AbsIntEvaluator get(DriverContext context) {
      return new AbsIntEvaluator(fieldVal.get(context), context);
    }

    @Override
    public String toString() {
      return "AbsIntEvaluator[" + "fieldVal=" + fieldVal + "]";
    }
  }
}
