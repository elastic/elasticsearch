// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Intersects}.
 * This class is generated. Do not edit it.
 */
public final class IntersectsConstantStringEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Geometry leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  public IntersectsConstantStringEvaluator(Geometry leftValue,
      EvalOperator.ExpressionEvaluator rightValue) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
  }

  @Override
  public Block eval(Page page) {
    Block rightValueUncastBlock = rightValue.eval(page);
    if (rightValueUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock rightValueBlock = (BytesRefBlock) rightValueUncastBlock;
    BytesRefVector rightValueVector = rightValueBlock.asVector();
    if (rightValueVector == null) {
      return eval(page.getPositionCount(), rightValueBlock);
    }
    return eval(page.getPositionCount(), rightValueVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock rightValueBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef rightValueScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (rightValueBlock.isNull(p) || rightValueBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(Intersects.processConstant(leftValue, rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch)));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BytesRefVector rightValueVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef rightValueScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(Intersects.processConstant(leftValue, rightValueVector.getBytesRef(p, rightValueScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "IntersectsConstantStringEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }
}
