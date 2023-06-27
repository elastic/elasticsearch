// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Now}.
 * This class is generated. Do not edit it.
 */
public final class NowEvaluator implements EvalOperator.ExpressionEvaluator {
  private final long now;

  public NowEvaluator(long now) {
    this.now = now;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  public LongVector eval(int positionCount) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(Now.process(now));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "NowEvaluator[" + "now=" + now + "]";
  }
}
