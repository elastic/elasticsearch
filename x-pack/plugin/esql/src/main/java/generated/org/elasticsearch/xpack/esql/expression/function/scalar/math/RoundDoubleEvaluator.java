// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Double;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Do not edit it.
 */
public final class RoundDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  public RoundDoubleEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals) {
    this.val = val;
    this.decimals = decimals;
  }

  static Double fold(Expression val, Expression decimals) {
    Object valVal = val.fold();
    if (valVal == null) {
      return null;
    }
    Object decimalsVal = decimals.fold();
    if (decimalsVal == null) {
      return null;
    }
    return Round.process((double) valVal, (long) decimalsVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    if (valVal == null) {
      return null;
    }
    Object decimalsVal = decimals.computeRow(page, position);
    if (decimalsVal == null) {
      return null;
    }
    return Round.process((double) valVal, (long) decimalsVal);
  }

  @Override
  public String toString() {
    return "RoundDoubleEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
  }
}
