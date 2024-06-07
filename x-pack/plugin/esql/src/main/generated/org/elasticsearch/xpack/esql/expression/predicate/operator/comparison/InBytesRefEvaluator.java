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
import org.elasticsearch.compute.data.BytesRefBlock;
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
public final class InBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator[] rhs;

  private final DriverContext driverContext;

  public InBytesRefEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator[] rhs, DriverContext driverContext) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock lhsBlock = (BytesRefBlock) lhs.eval(page)) {
      BytesRefBlock[] rhsBlocks = new BytesRefBlock[rhs.length];
      try (Releasable rhsRelease = Releasables.wrap(rhsBlocks)) {
        for (int i = 0; i < rhsBlocks.length; i++) {
          rhsBlocks[i] = (BytesRefBlock)rhs[i].eval(page);
        }
        return eval(page.getPositionCount(), lhsBlock, rhsBlocks);
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock lhsBlock, BytesRefBlock[] rhsBlocks) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!lhsBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        aba: for (int i = 0; i < rhsBlocks.length; i++) {
          if (!rhsBlocks[i].isNull(p)) {
            allBlocksAreNulls = false;
            break aba;
          }
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          In.process(result, p, lhsBlock, rhsBlocks);
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "InBytesRefEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
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
    public InBytesRefEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] rhs = Arrays.stream(this.rhs).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new InBytesRefEvaluator(source, lhs.get(context), rhs, context);
    }

    @Override
    public String toString() {
      return "InBytesRefEvaluator[" + "lhs=" + lhs + ", rhs=" + Arrays.toString(rhs) + "]";
    }
  }
}
