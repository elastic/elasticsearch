// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Integer;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Length}.
 * This class is generated. Do not edit it.
 */
public final class LengthEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  public LengthEvaluator(EvalOperator.ExpressionEvaluator val) {
    this.val = val;
  }

  static Integer process(Object valVal) {
    if (valVal == null) {
      return null;
    }
    return Length.process((BytesRef) valVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object valVal = val.computeRow(page, position);
    return process(valVal);
  }

  @Override
  public String toString() {
    return "LengthEvaluator[" + "val=" + val + "]";
  }
}
