// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Div}.
 * This class is generated. Do not edit it.
 */
public final class DivDoublesEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  public DivDoublesEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs) {
    this.warnings = new Warnings(source);
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public Block eval(Page page) {
    Block lhsUncastBlock = lhs.eval(page);
    if (lhsUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    DoubleBlock lhsBlock = (DoubleBlock) lhsUncastBlock;
    Block rhsUncastBlock = rhs.eval(page);
    if (rhsUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    DoubleBlock rhsBlock = (DoubleBlock) rhsUncastBlock;
    DoubleVector lhsVector = lhsBlock.asVector();
    if (lhsVector == null) {
      return eval(page.getPositionCount(), lhsBlock, rhsBlock);
    }
    DoubleVector rhsVector = rhsBlock.asVector();
    if (rhsVector == null) {
      return eval(page.getPositionCount(), lhsBlock, rhsBlock);
    }
    return eval(page.getPositionCount(), lhsVector, rhsVector);
  }

  public DoubleBlock eval(int positionCount, DoubleBlock lhsBlock, DoubleBlock rhsBlock) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (lhsBlock.isNull(p) || lhsBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (rhsBlock.isNull(p) || rhsBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendDouble(Div.processDoubles(lhsBlock.getDouble(lhsBlock.getFirstValueIndex(p)), rhsBlock.getDouble(rhsBlock.getFirstValueIndex(p))));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  public DoubleBlock eval(int positionCount, DoubleVector lhsVector, DoubleVector rhsVector) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendDouble(Div.processDoubles(lhsVector.getDouble(p), rhsVector.getDouble(p)));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "DivDoublesEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }
}
