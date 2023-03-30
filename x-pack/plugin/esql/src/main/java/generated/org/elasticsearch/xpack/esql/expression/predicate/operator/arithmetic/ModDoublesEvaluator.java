// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.Double;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Mod}.
 * This class is generated. Do not edit it.
 */
public final class ModDoublesEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  public ModDoublesEvaluator(EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  static Double fold(Expression lhs, Expression rhs) {
    Object lhsVal = lhs.fold();
    if (lhsVal == null) {
      return null;
    }
    Object rhsVal = rhs.fold();
    if (rhsVal == null) {
      return null;
    }
    return Mod.processDoubles((double) lhsVal, (double) rhsVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object lhsVal = lhs.computeRow(page, position);
    if (lhsVal == null) {
      return null;
    }
    Object rhsVal = rhs.computeRow(page, position);
    if (rhsVal == null) {
      return null;
    }
    return Mod.processDoubles((double) lhsVal, (double) rhsVal);
  }

  @Override
  public String toString() {
    return "ModDoublesEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }
}
