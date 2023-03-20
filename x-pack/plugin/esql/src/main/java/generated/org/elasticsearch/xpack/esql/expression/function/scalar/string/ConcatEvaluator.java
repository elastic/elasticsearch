// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Concat}.
 * This class is generated. Do not edit it.
 */
public final class ConcatEvaluator implements EvalOperator.ExpressionEvaluator {
  private final BytesRefBuilder scratch;

  private final BytesRef[] valuesVal;

  private final EvalOperator.ExpressionEvaluator[] values;

  public ConcatEvaluator(BytesRefBuilder scratch, EvalOperator.ExpressionEvaluator[] values) {
    this.scratch = scratch;
    this.valuesVal = new BytesRef[values.length];
    this.values = values;
  }

  static BytesRef fold(BytesRefBuilder scratch, List<Expression> values) {
    BytesRef[] valuesVal = new BytesRef[values.size()];
    for (int i = 0; i < valuesVal.length; i++) {
      valuesVal[i] = (BytesRef) values.get(i).fold();
      if (valuesVal[i] == null) {
        return null;
      }
    }
    return Concat.process(scratch, valuesVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    for (int i = 0; i < valuesVal.length; i++) {
      valuesVal[i] = (BytesRef) values[i].computeRow(page, position);
      if (valuesVal[i] == null) {
        return null;
      }
    }
    return Concat.process(scratch, valuesVal);
  }

  @Override
  public String toString() {
    return "ConcatEvaluator[" + "values=" + Arrays.toString(values) + "]";
  }
}
