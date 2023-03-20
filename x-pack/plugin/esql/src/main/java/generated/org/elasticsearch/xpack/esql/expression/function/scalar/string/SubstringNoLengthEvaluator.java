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
public final class SubstringNoLengthEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator start;

  public SubstringNoLengthEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator start) {
    this.str = str;
    this.start = start;
  }

  static BytesRef fold(Expression str, Expression start) {
    Object strVal = str.fold();
    if (strVal == null) {
      return null;
    }
    Object startVal = start.fold();
    if (startVal == null) {
      return null;
    }
    return Substring.process((BytesRef) strVal, (int) startVal);
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
    return Substring.process((BytesRef) strVal, (int) startVal);
  }

  @Override
  public String toString() {
    return "SubstringNoLengthEvaluator[" + "str=" + str + ", start=" + start + "]";
  }
}
