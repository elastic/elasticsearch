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

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link IsFinite}.
 * This class is generated. Do not edit it.
 */
public final class IsFiniteEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  public IsFiniteEvaluator(EvalOperator.ExpressionEvaluator val) {
    this.val = val;
  }

  static Boolean process(Object valVal) {
    if (valVal == null) {
      return null;
    }
    return IsFinite.process((double) valVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    return process(valVal);
  }

  @Override
  public String toString() {
    return "IsFiniteEvaluator[" + "val=" + val + "]";
  }
}
