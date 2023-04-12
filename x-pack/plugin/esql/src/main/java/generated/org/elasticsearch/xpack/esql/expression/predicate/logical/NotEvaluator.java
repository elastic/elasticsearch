// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import java.lang.Boolean;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Not}.
 * This class is generated. Do not edit it.
 */
public final class NotEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator v;

  public NotEvaluator(EvalOperator.ExpressionEvaluator v) {
    this.v = v;
  }

  static Boolean fold(Expression v) {
    Object vVal = v.fold();
    if (vVal == null) {
      return null;
    }
    return Not.process((boolean) vVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object vVal = v.computeRow(page, position);
    if (vVal == null) {
      return null;
    }
    return Not.process((boolean) vVal);
  }

  @Override
  public String toString() {
    return "NotEvaluator[" + "v=" + v + "]";
  }
}
