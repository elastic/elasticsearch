// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Do not edit it.
 */
public final class AbsLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  public AbsLongEvaluator(EvalOperator.ExpressionEvaluator fieldVal) {
    this.fieldVal = fieldVal;
  }

  @Override
  public Block eval(Page page) {
    Block fieldValUncastBlock = fieldVal.eval(page);
    if (fieldValUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    LongBlock fieldValBlock = (LongBlock) fieldValUncastBlock;
    LongVector fieldValVector = fieldValBlock.asVector();
    if (fieldValVector == null) {
      return eval(page.getPositionCount(), fieldValBlock);
    }
    return eval(page.getPositionCount(), fieldValVector).asBlock();
  }

  public LongBlock eval(int positionCount, LongBlock fieldValBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (fieldValBlock.isNull(p) || fieldValBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendLong(Abs.process(fieldValBlock.getLong(fieldValBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public LongVector eval(int positionCount, LongVector fieldValVector) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(Abs.process(fieldValVector.getLong(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "AbsLongEvaluator[" + "fieldVal=" + fieldVal + "]";
  }
}
