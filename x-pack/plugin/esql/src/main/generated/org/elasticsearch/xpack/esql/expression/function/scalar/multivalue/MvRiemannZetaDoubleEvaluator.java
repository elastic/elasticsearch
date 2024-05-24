// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvRiemannZeta}.
 * This class is generated. Do not edit it.
 */
public final class MvRiemannZetaDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator block;

  private final EvalOperator.ExpressionEvaluator p;

  private final DriverContext driverContext;

  public MvRiemannZetaDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator block,
      EvalOperator.ExpressionEvaluator p, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.block = block;
    this.p = p;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock blockBlock = (DoubleBlock) block.eval(page)) {
      try (DoubleBlock pBlock = (DoubleBlock) p.eval(page)) {
        return eval(page.getPositionCount(), blockBlock, pBlock);
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock blockBlock, DoubleBlock pBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!blockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (pBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (pBlock.getValueCount(p) != 1) {
          if (pBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          MvRiemannZeta.process(result, p, blockBlock, pBlock.getDouble(pBlock.getFirstValueIndex(p)));
        } catch (InvalidArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvRiemannZetaDoubleEvaluator[" + "block=" + block + ", p=" + p + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(block, p);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory block;

    private final EvalOperator.ExpressionEvaluator.Factory p;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory block,
        EvalOperator.ExpressionEvaluator.Factory p) {
      this.source = source;
      this.block = block;
      this.p = p;
    }

    @Override
    public MvRiemannZetaDoubleEvaluator get(DriverContext context) {
      return new MvRiemannZetaDoubleEvaluator(source, block.get(context), p.get(context), context);
    }

    @Override
    public String toString() {
      return "MvRiemannZetaDoubleEvaluator[" + "block=" + block + ", p=" + p + "]";
    }
  }
}
