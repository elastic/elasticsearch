// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Cast}.
 * This class is generated. Do not edit it.
 */
public final class CastLongToUnsignedLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator v;

  private final DriverContext driverContext;

  public CastLongToUnsignedLongEvaluator(EvalOperator.ExpressionEvaluator v,
      DriverContext driverContext) {
    this.v = v;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock vBlock = (LongBlock) v.eval(page)) {
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        return eval(page.getPositionCount(), vBlock);
      }
      return eval(page.getPositionCount(), vVector).asBlock();
    }
  }

  public LongBlock eval(int positionCount, LongBlock vBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p) || vBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendLong(Cast.castLongToUnsignedLong(vBlock.getLong(vBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector vVector) {
    try(LongVector.Builder result = driverContext.blockFactory().newLongVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(Cast.castLongToUnsignedLong(vVector.getLong(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CastLongToUnsignedLongEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory v;

    public Factory(EvalOperator.ExpressionEvaluator.Factory v) {
      this.v = v;
    }

    @Override
    public CastLongToUnsignedLongEvaluator get(DriverContext context) {
      return new CastLongToUnsignedLongEvaluator(v.get(context), context);
    }

    @Override
    public String toString() {
      return "CastLongToUnsignedLongEvaluator[" + "v=" + v + "]";
    }
  }
}
