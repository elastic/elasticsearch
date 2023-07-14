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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Intersects}.
 * This class is generated. Do not edit it.
 */
public final class IntersectsLongStringEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  public IntersectsLongStringEvaluator(EvalOperator.ExpressionEvaluator leftValue,
      EvalOperator.ExpressionEvaluator rightValue) {
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
    Block rightValueUncastBlock = rightValue.eval(page);
    if (rightValueUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock rightValueBlock = (BytesRefBlock) rightValueUncastBlock;
    LongVector leftValueVector = leftValueBlock.asVector();
    if (leftValueVector == null) {
      return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
    }
    BytesRefVector rightValueVector = rightValueBlock.asVector();
    if (rightValueVector == null) {
      return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
    }
    return eval(page.getPositionCount(), leftValueVector, rightValueVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock,
      BytesRefBlock rightValueBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef rightValueScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (leftValueBlock.isNull(p) || leftValueBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (rightValueBlock.isNull(p) || rightValueBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(Intersects.process(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValueBlock.getBytesRef(rightValueBlock.getFirstValueIndex(p), rightValueScratch)));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, LongVector leftValueVector,
      BytesRefVector rightValueVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef rightValueScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(Intersects.process(leftValueVector.getLong(p), rightValueVector.getBytesRef(p, rightValueScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "IntersectsLongStringEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }
}
