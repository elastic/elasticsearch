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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Cast}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CastUnsignedLongToDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator v;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CastUnsignedLongToDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator v,
      DriverContext driverContext) {
    this.source = source;
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

  public DoubleBlock eval(int positionCount, LongBlock vBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (vBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (vBlock.getValueCount(p) != 1) {
          if (vBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendDouble(Cast.castUnsignedLongToDouble(vBlock.getLong(vBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, LongVector vVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendDouble(p, Cast.castUnsignedLongToDouble(vVector.getLong(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CastUnsignedLongToDoubleEvaluator[" + "v=" + v + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(v);
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

    private final EvalOperator.ExpressionEvaluator.Factory v;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory v) {
      this.source = source;
      this.v = v;
    }

    @Override
    public CastUnsignedLongToDoubleEvaluator get(DriverContext context) {
      return new CastUnsignedLongToDoubleEvaluator(source, v.get(context), context);
    }

    @Override
    public String toString() {
      return "CastUnsignedLongToDoubleEvaluator[" + "v=" + v + "]";
    }
  }
}
