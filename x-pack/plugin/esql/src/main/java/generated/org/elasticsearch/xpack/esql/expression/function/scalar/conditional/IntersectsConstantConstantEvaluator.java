// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Intersects}.
 * This class is generated. Do not edit it.
 */
public final class IntersectsConstantConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Geometry leftValue;

  private final Geometry rightValue;

  public IntersectsConstantConstantEvaluator(Geometry leftValue, Geometry rightValue) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount()).asBlock();
  }

  public BooleanVector eval(int positionCount) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(Intersects.processConstant(leftValue, rightValue));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "IntersectsConstantConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }
}
