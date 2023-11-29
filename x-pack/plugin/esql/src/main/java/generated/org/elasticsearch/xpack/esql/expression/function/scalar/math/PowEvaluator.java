// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Do not edit it.
 */
public final class PowEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator exponent;

  private final DriverContext driverContext;

  public PowEvaluator(Source source, EvalOperator.ExpressionEvaluator base,
      EvalOperator.ExpressionEvaluator exponent, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.base = base;
    this.exponent = exponent;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock baseBlock = (DoubleBlock) base.eval(page)) {
      try (DoubleBlock exponentBlock = (DoubleBlock) exponent.eval(page)) {
        DoubleVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        DoubleVector exponentVector = exponentBlock.asVector();
        if (exponentVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        return eval(page.getPositionCount(), baseVector, exponentVector);
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock exponentBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (baseBlock.isNull(p) || baseBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        if (exponentBlock.isNull(p) || exponentBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        try {
          result.appendDouble(Pow.process(baseBlock.getDouble(baseBlock.getFirstValueIndex(p)), exponentBlock.getDouble(exponentBlock.getFirstValueIndex(p))));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector baseVector, DoubleVector exponentVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(Pow.process(baseVector.getDouble(p), exponentVector.getDouble(p)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(base, exponent);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory base;

    private final EvalOperator.ExpressionEvaluator.Factory exponent;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory base,
        EvalOperator.ExpressionEvaluator.Factory exponent) {
      this.source = source;
      this.base = base;
      this.exponent = exponent;
    }

    @Override
    public PowEvaluator get(DriverContext context) {
      return new PowEvaluator(source, base.get(context), exponent.get(context), context);
    }

    @Override
    public String toString() {
      return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
    }
  }
}
