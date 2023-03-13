// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Number;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Do not edit it.
 */
public final class RoundEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  public RoundEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals) {
    this.val = val;
    this.decimals = decimals;
  }

  static Number process(Object valVal, Object decimalsVal) {
    if (valVal == null) {
      return null;
    }
    if (decimalsVal == null) {
      return null;
    }
    return Round.process((Number) valVal, (Number) decimalsVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    Object decimalsVal = decimals.computeRow(page, position);
    return process(valVal, decimalsVal);
  }

  @Override
  public String toString() {
    return "RoundEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
  }
}
