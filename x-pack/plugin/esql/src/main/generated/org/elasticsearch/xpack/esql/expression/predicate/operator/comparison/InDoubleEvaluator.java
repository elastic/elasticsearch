// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link In}.
 * This class is generated. Do not edit it.
 */
public final class InDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator[] rhs;

  private final DriverContext driverContext;

  public InDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator[] rhs, DriverContext driverContext) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock lhsBlock = (DoubleBlock) lhs.eval(page)) {
      DoubleBlock[] rhsBlocks = new DoubleBlock[rhs.length];
      try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
        for (int i = 0; i < rhsBlocks.length; i++) {
          rhsBlocks[i] = (DoubleBlock)rhs[i].eval(page);
        }
        DoubleVector lhsVector = lhsBlock.asVector();
        if (lhsVector == null) {
          return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
        }
        DoubleVector[] rhsVectors = new DoubleVector[rhs.length];
        for (int i = 0; i < rhsBlocks.length; i++) {
          rhsVectors[i] = rhsBlocks[i].asVector();
          if (rhsVectors[i] == null) {
            return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
          }
        }
        return eval(page.getPositionCount(), lhsVector, rhsVectors);
      }
    }
  }

  public BooleanBlock eval(int positionCount, DoubleBlock lhsBlock, DoubleBlock[] rhsBlocks) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      double[] rhsValues = new double[rhs.length];
      position: for (int p = 0; p < positionCount; p++) {
        if (lhsBlock.getValueCount(p) > 1) {
          warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
        }
        for (int i = 0; i < rhsBlocks.length; i++) {
          if (rhsBlocks[i].getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
        }
        // unpack rhsBlocks into rhsValues
        for (int i = 0; i < rhsBlocks.length; i++) {
          int o = rhsBlocks[i].getFirstValueIndex(p);
          rhsValues[i] = rhsBlocks[i].isNull(p) ? null : rhsBlocks[i].getDouble(o);
        }
        In.process(result, lhsBlock.getDouble(lhsBlock.getFirstValueIndex(p)), rhsValues);
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, DoubleVector lhsVector, DoubleVector[] rhsVectors) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      double[] rhsValues = new double[rhs.length];
      position: for (int p = 0; p < positionCount; p++) {
        // unpack rhsVectors into rhsValues
        for (int i = 0; i < rhsVectors.length; i++) {
          rhsValues[i] = rhsVectors[i].getDouble(p);
        }
        In.process(result, lhsVector.getDouble(p), rhsValues);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "InDoubleEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs, () -> Releasables.close(rhs));
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory lhs;

    private final EvalOperator.ExpressionEvaluator.Factory[] rhs;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory[] rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public InDoubleEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new InDoubleEvaluator(source, lhs.get(context), rhs, context);
    }

    @Override
    public String toString() {
      return "InDoubleEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }
  }
}
