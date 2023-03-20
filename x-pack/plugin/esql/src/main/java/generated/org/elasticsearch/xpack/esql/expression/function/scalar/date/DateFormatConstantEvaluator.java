// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateFormat}.
 * This class is generated. Do not edit it.
 */
public final class DateFormatConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final DateFormatter formatter;

  public DateFormatConstantEvaluator(EvalOperator.ExpressionEvaluator val,
      DateFormatter formatter) {
    this.val = val;
    this.formatter = formatter;
  }

  static BytesRef fold(Expression val, DateFormatter formatter) {
    Object valVal = val.fold();
    if (valVal == null) {
      return null;
    }
    return DateFormat.process((long) valVal, formatter);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    if (valVal == null) {
      return null;
    }
    return DateFormat.process((long) valVal, formatter);
  }

  @Override
  public String toString() {
    return "DateFormatConstantEvaluator[" + "val=" + val + ", formatter=" + formatter + "]";
  }
}
