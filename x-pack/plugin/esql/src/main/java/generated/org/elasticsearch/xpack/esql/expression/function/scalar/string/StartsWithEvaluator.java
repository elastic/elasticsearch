// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Boolean;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StartsWith}.
 * This class is generated. Do not edit it.
 */
public final class StartsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator prefix;

  public StartsWithEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator prefix) {
    this.str = str;
    this.prefix = prefix;
  }

  static Boolean process(Object strVal, Object prefixVal) {
    if (strVal == null) {
      return null;
    }
    if (prefixVal == null) {
      return null;
    }
    return StartsWith.process((BytesRef) strVal, (BytesRef) prefixVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object strVal = str.computeRow(page, position);
    Object prefixVal = prefix.computeRow(page, position);
    return process(strVal, prefixVal);
  }

  @Override
  public String toString() {
    return "StartsWithEvaluator[" + "str=" + str + ", prefix=" + prefix + "]";
  }
}
