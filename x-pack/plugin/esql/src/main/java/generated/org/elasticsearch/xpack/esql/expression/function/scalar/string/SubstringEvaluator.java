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

  static BytesRef process(Object strVal, Object startVal, Object lengthVal) {
    if (strVal == null) {
      return null;
    }
    if (startVal == null) {
      return null;
    }
    if (lengthVal == null) {
      return null;
    }
    return Substring.process((BytesRef) strVal, (int) startVal, (int) lengthVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object strVal = str.computeRow(page, position);
    Object startVal = start.computeRow(page, position);
    Object lengthVal = length.computeRow(page, position);
    return process(strVal, startVal, lengthVal);
  }

  @Override
  public String toString() {
    return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
  }
}
