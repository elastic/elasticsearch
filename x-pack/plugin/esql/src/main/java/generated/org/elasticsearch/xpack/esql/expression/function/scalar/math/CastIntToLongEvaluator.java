// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Cast}.
 * This class is generated. Do not edit it.
 */
public final class CastIntToLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator v;

  private final DriverContext driverContext;

  public CastIntToLongEvaluator(Source source, EvalOperator.ExpressionEvaluator v,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.v = v;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock vBlock = (IntBlock) v.eval(page)) {
      IntVector vVector = vBlock.asVector();
      if (vVector == null) {
        return eval(page.getPositionCount(), vBlock);
      }
      return eval(page.getPositionCount(), vVector).asBlock();
    }
  }

  public LongBlock eval(int positionCount, IntBlock vBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (vBlock.getValueCount(p) != 1) {
          if (vBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendLong(Cast.castIntToLong(vBlock.getInt(vBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, IntVector vVector) {
    try(LongVector.Builder result = driverContext.blockFactory().newLongVectorBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(Cast.castIntToLong(vVector.getInt(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CastIntToLongEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory v;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory v) {
      this.source = source;
      this.v = v;
    }

    @Override
    public CastIntToLongEvaluator get(DriverContext context) {
      return new CastIntToLongEvaluator(source, v.get(context), context);
    }

    @Override
    public String toString() {
      return "CastIntToLongEvaluator[" + "v=" + v + "]";
    }
  }
}
