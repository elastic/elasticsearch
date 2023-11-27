// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

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
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Do not edit it.
 */
public final class RoundDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  private final DriverContext driverContext;

  public RoundDoubleEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals, DriverContext driverContext) {
    this.val = val;
    this.decimals = decimals;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock valBlock = (DoubleBlock) val.eval(page)) {
      try (LongBlock decimalsBlock = (LongBlock) decimals.eval(page)) {
        DoubleVector valVector = valBlock.asVector();
        if (valVector == null) {
          return eval(page.getPositionCount(), valBlock, decimalsBlock);
        }
        LongVector decimalsVector = decimalsBlock.asVector();
        if (decimalsVector == null) {
          return eval(page.getPositionCount(), valBlock, decimalsBlock);
        }
        return eval(page.getPositionCount(), valVector, decimalsVector).asBlock();
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock valBlock, LongBlock decimalsBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        if (decimalsBlock.isNull(p) || decimalsBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendDouble(Round.process(valBlock.getDouble(valBlock.getFirstValueIndex(p)), decimalsBlock.getLong(decimalsBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector valVector, LongVector decimalsVector) {
    try(DoubleVector.Builder result = driverContext.blockFactory().newDoubleVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(Round.process(valVector.getDouble(p), decimalsVector.getLong(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundDoubleEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val, decimals);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final EvalOperator.ExpressionEvaluator.Factory decimals;

    public Factory(EvalOperator.ExpressionEvaluator.Factory val,
        EvalOperator.ExpressionEvaluator.Factory decimals) {
      this.val = val;
      this.decimals = decimals;
    }

    @Override
    public RoundDoubleEvaluator get(DriverContext context) {
      return new RoundDoubleEvaluator(val.get(context), decimals.get(context), context);
    }

    @Override
    public String toString() {
      return "RoundDoubleEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
    }
  }
}
