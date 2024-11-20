// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link NotEquals}.
 * This class is generated. Do not edit it.
 */
public final class NotEqualsLongsEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NotEqualsLongsEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs, DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock lhsBlock = (LongBlock) lhs.eval(page)) {
      try (LongBlock rhsBlock = (LongBlock) rhs.eval(page)) {
        LongVector lhsVector = lhsBlock.asVector();
        if (lhsVector == null) {
          return eval(page.getPositionCount(), lhsBlock, rhsBlock);
        }
        LongVector rhsVector = rhsBlock.asVector();
        if (rhsVector == null) {
          return eval(page.getPositionCount(), lhsBlock, rhsBlock);
        }
        return eval(page.getPositionCount(), lhsVector, rhsVector).asBlock();
      }
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock lhsBlock, LongBlock rhsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (lhsBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (lhsBlock.getValueCount(p) != 1) {
          if (lhsBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (rhsBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (rhsBlock.getValueCount(p) != 1) {
          if (rhsBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBoolean(NotEquals.processLongs(lhsBlock.getLong(lhsBlock.getFirstValueIndex(p)), rhsBlock.getLong(rhsBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, LongVector lhsVector, LongVector rhsVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(p, NotEquals.processLongs(lhsVector.getLong(p), rhsVector.getLong(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NotEqualsLongsEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs, rhs);
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

    private final EvalOperator.ExpressionEvaluator.Factory lhs;

    private final EvalOperator.ExpressionEvaluator.Factory rhs;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public NotEqualsLongsEvaluator get(DriverContext context) {
      return new NotEqualsLongsEvaluator(source, lhs.get(context), rhs.get(context), context);
    }

    @Override
    public String toString() {
      return "NotEqualsLongsEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
