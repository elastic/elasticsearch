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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.qlcore.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Do not edit it.
 */
public final class PowIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator exponent;

  private final DriverContext driverContext;

  public PowIntEvaluator(Source source, EvalOperator.ExpressionEvaluator base,
      EvalOperator.ExpressionEvaluator exponent, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.base = base;
    this.exponent = exponent;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref baseRef = base.eval(page)) {
      DoubleBlock baseBlock = (DoubleBlock) baseRef.block();
      try (Block.Ref exponentRef = exponent.eval(page)) {
        DoubleBlock exponentBlock = (DoubleBlock) exponentRef.block();
        DoubleVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), baseBlock, exponentBlock));
        }
        DoubleVector exponentVector = exponentBlock.asVector();
        if (exponentVector == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), baseBlock, exponentBlock));
        }
        return Block.Ref.floating(eval(page.getPositionCount(), baseVector, exponentVector));
      }
    }
  }

  public IntBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock exponentBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (baseBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (baseBlock.getValueCount(p) != 1) {
          if (baseBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (exponentBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (exponentBlock.getValueCount(p) != 1) {
          if (exponentBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendInt(Pow.processInt(baseBlock.getDouble(baseBlock.getFirstValueIndex(p)), exponentBlock.getDouble(exponentBlock.getFirstValueIndex(p))));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, DoubleVector baseVector, DoubleVector exponentVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendInt(Pow.processInt(baseVector.getDouble(p), exponentVector.getDouble(p)));
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
    return "PowIntEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
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
    public PowIntEvaluator get(DriverContext context) {
      return new PowIntEvaluator(source, base.get(context), exponent.get(context), context);
    }

    @Override
    public String toString() {
      return "PowIntEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
    }
  }
}
