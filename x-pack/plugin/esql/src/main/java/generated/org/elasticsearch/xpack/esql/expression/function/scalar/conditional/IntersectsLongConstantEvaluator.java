// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Intersects}.
 * This class is generated. Do not edit it.
 */
public final class IntersectsLongConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator leftValue;

  private final String rightValue;

  public IntersectsLongConstantEvaluator(EvalOperator.ExpressionEvaluator leftValue,
      String rightValue) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
  }

  @Override
  public Block eval(Page page) {
    Block leftValueUncastBlock = leftValue.eval(page);
    if (leftValueUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    LongBlock leftValueBlock = (LongBlock) leftValueUncastBlock;
    LongVector leftValueVector = leftValueBlock.asVector();
    if (leftValueVector == null) {
      return eval(page.getPositionCount(), leftValueBlock);
    }
    return eval(page.getPositionCount(), leftValueVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (leftValueBlock.isNull(p) || leftValueBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(Intersects.processConstant(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValue));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, LongVector leftValueVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(Intersects.processConstant(leftValueVector.getLong(p), rightValue));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "IntersectsLongConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }
}
