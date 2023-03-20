// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Long;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateTrunc}.
 * This class is generated. Do not edit it.
 */
public final class DateTruncEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  private final Rounding.Prepared rounding;

  public DateTruncEvaluator(EvalOperator.ExpressionEvaluator fieldVal, Rounding.Prepared rounding) {
    this.fieldVal = fieldVal;
    this.rounding = rounding;
  }

  static Long fold(Expression fieldVal, Rounding.Prepared rounding) {
    Object fieldValVal = fieldVal.fold();
    if (fieldValVal == null) {
      return null;
    }
    return DateTrunc.process((long) fieldValVal, rounding);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object fieldValVal = fieldVal.computeRow(page, position);
    if (fieldValVal == null) {
      return null;
    }
    return DateTrunc.process((long) fieldValVal, rounding);
  }

  @Override
  public String toString() {
    return "DateTruncEvaluator[" + "fieldVal=" + fieldVal + ", rounding=" + rounding + "]";
  }
}
