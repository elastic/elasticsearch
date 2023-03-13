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

  static BytesRef process(Object strVal, Object startVal) {
    if (strVal == null) {
      return null;
    }
    if (startVal == null) {
      return null;
    }
    return Substring.process((BytesRef) strVal, (int) startVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object strVal = str.computeRow(page, position);
    Object startVal = start.computeRow(page, position);
    return process(strVal, startVal);
  }

  @Override
  public String toString() {
    return "SubstringNoLengthEvaluator[" + "str=" + str + ", start=" + start + "]";
  }
}
