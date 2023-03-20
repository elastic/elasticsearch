// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Boolean;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link IsInfinite}.
 * This class is generated. Do not edit it.
 */
public final class IsInfiniteEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  public IsInfiniteEvaluator(EvalOperator.ExpressionEvaluator val) {
    this.val = val;
  }

  static Boolean fold(Expression val) {
    Object valVal = val.fold();
    if (valVal == null) {
      return null;
    }
    return IsInfinite.process((double) valVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    if (valVal == null) {
      return null;
    }
    return IsInfinite.process((double) valVal);
  }

  @Override
  public String toString() {
    return "IsInfiniteEvaluator[" + "val=" + val + "]";
  }
}
