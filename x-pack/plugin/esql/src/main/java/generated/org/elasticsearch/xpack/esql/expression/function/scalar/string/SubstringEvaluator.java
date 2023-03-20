// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Substring}.
 * This class is generated. Do not edit it.
 */
public final class SubstringEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator start;

  private final EvalOperator.ExpressionEvaluator length;

  public SubstringEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator start, EvalOperator.ExpressionEvaluator length) {
    this.str = str;
    this.start = start;
    this.length = length;
  }

  static BytesRef fold(Expression str, Expression start, Expression length) {
    Object strVal = str.fold();
    if (strVal == null) {
      return null;
    }
    Object startVal = start.fold();
    if (startVal == null) {
      return null;
    }
    Object lengthVal = length.fold();
    if (lengthVal == null) {
      return null;
    }
    return Substring.process((BytesRef) strVal, (int) startVal, (int) lengthVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object strVal = str.computeRow(page, position);
    if (strVal == null) {
      return null;
    }
    Object startVal = start.computeRow(page, position);
    if (startVal == null) {
      return null;
    }
    Object lengthVal = length.computeRow(page, position);
    if (lengthVal == null) {
      return null;
    }
    return Substring.process((BytesRef) strVal, (int) startVal, (int) lengthVal);
  }

  @Override
  public String toString() {
    return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
  }
}
